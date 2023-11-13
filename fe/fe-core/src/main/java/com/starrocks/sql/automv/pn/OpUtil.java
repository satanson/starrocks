// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.automv.pn;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.generator.ColumnAlias;
import com.starrocks.sql.automv.generator.ColumnRefToIdConverter;
import com.starrocks.sql.automv.pieces.DerivedColumn;
import com.starrocks.sql.automv.pieces.GenericColumn;
import com.starrocks.sql.automv.util.EitherOr;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OpUtil {

    public static Optional<StrictOp> getExpr(GenericColumn column) {
        return column.cast(DerivedColumn.class).map(DerivedColumn::getExpr);
    }

    public static Function<Op, String> toOpToSqlConverter(TieredMap<Integer, ColumnAlias> columnAliases) {
        return op -> Transcriptase.transcript(op, columnAliases);
    }

    public static Optional<List<String>> conjunctsToSql(List<Op> conjuncts,
                                                        Function<Op, String> opToSql) {
        if (conjuncts.isEmpty()) {
            return Optional.empty();
        } else {
            List<String> sqlList = conjuncts.stream().map(opToSql).collect(Collectors.toList());
            return Optional.of(sqlList);
        }
    }

    public static StrictOp mustGetExpr(GenericColumn column) {
        return Objects.requireNonNull(getExpr(column).orElse(null));
    }

    public static Optional<Op> getOp(GenericColumn column) {
        return column.cast(DerivedColumn.class).map(c -> c.getExpr().getOp());
    }

    public static Op mustGetOp(GenericColumn column) {
        return Objects.requireNonNull(getOp(column).orElse(null));
    }

    public static boolean isFun(GenericColumn column, String name) {
        return getExpr(column).map(StrictOp::getOp).map(c -> c.isFun(name)).orElse(false);
    }

    public static boolean isAvg(GenericColumn column) {
        return isFun(column, FunctionSet.AVG);
    }

    public static boolean isAvgDistinct(GenericColumn column) {
        return isDistinct(column) && isAvg(column);
    }

    public static boolean isSumDistinct(GenericColumn column) {
        return isDistinct(column) && isSum(column);
    }

    public static boolean isCountDistinct(GenericColumn column) {
        return isDistinct(column) && isCount(column);
    }

    public static boolean isCountOrSumDistinct(GenericColumn column) {
        return isCountDistinct(column) || isSumDistinct(column);
    }

    public static boolean isArrayAggDistinct(GenericColumn column) {
        return isFun(column, FunctionSet.ARRAY_AGG_DISTINCT);
    }

    public static Optional<OpPlus2> rewriteDistinctByArrayAggDistinct(
            OpPlus distinctOpPlus, Supplier<Integer> idGen, Map<StrictOp, Integer> alreadyExists) {
        int distinctId = distinctOpPlus.getId();
        Op distinctOp = distinctOpPlus.getOp();
        Type type = distinctOp.getType();
        Preconditions.checkArgument(distinctOp.isSumDistinct() || distinctOp.isCountDistinct());
        List<Op> args = distinctOp.arg(0).getArgs();
        Preconditions.checkArgument(!args.isEmpty());
        Op arg = args.get(0);

        Type arrayType = new ArrayType(arg.getType());
        Op arrayAggDistinctOp =
                Apply.apply(arrayType, FunctionSet.ARRAY_AGG, true,
                        Apply.apply(arrayType, BuiltinKind.DISTINCT, true, args));
        EitherOr<OpPlus> arrayAggDistinctOpPlus = Optional.ofNullable(alreadyExists.get(arrayAggDistinctOp.strict()))
                .map(id -> EitherOr.either(OpPlus.of(arrayAggDistinctOp, id)))
                .orElseGet(() -> EitherOr.or(OpPlus.of(arrayAggDistinctOp, idGen.get())));

        List<EitherOr<OpPlus>> argPlusList = ImmutableList.of(arrayAggDistinctOpPlus);
        // array references to array_agg_distinct(col)
        Op var = arrayAggDistinctOpPlus.get().toVar();
        if (distinctOp.isSumDistinct()) {
            Op arraySumOp = Apply.apply(type, FunctionSet.ARRAY_SUM, true, var);
            return Optional.of(OpPlus2.of(OpPlus.of(arraySumOp, distinctId), argPlusList));
        } else {
            List<Op> locals = Op.getLocals(0, 1);
            Op local0 = locals.get(0);
            // x IS NOT NULL
            Op isNotNullOp = local0.toIsNotNull();
            // x->x IS NOT NULL
            Op lambdaIsNotNullOp = Op.simpleLambda(isNotNullOp, locals);
            // array_filter(x->x IS NOT NULL, array)
            Op arrayFilterOp = Op.apply(arrayType, FunctionSet.ARRAY_FILTER, true, lambdaIsNotNullOp, var);
            // array_length(array_filter(x->x IS NOT NULL, array))
            Op arrayLengthOp = Op.apply(type, FunctionSet.ARRAY_LENGTH, true, arrayFilterOp);
            return Optional.of(OpPlus2.of(OpPlus.of(arrayLengthOp, distinctId), argPlusList));
        }
    }

    public static Optional<OpPlus2> rewriteDistinctByBitmapAgg(
            OpPlus distinctOpPlus, Supplier<Integer> idGen, Map<StrictOp, Integer> alreadyExists) {
        int distinctId = distinctOpPlus.getId();
        Op distinctOp = distinctOpPlus.getOp();
        Type type = distinctOp.getType();
        Preconditions.checkArgument(distinctOp.isSumDistinct() || distinctOp.isCountDistinct());
        List<Op> args = distinctOp.arg(0).getArgs();
        Preconditions.checkArgument(!args.isEmpty());
        Type argType = args.get(0).getType();

        if (!argType.isBoolean() && (!argType.isIntegerType() || argType.isBigint())) {
            return Optional.empty();
        }

        Op bitmapAggOp = Apply.apply(Type.BITMAP, FunctionSet.BITMAP_AGG, true, args);
        EitherOr<OpPlus> bitmapAggOpPlus = Optional.ofNullable(alreadyExists.get(bitmapAggOp.strict()))
                .map(id -> EitherOr.either(OpPlus.of(bitmapAggOp, id)))
                .orElseGet(() -> EitherOr.or(OpPlus.of(bitmapAggOp, idGen.get())));
        Op var = bitmapAggOpPlus.get().toVar();
        if (distinctOp.isCountDistinct()) {
            Op bitmapCountOp = Apply.apply(type, FunctionSet.BITMAP_COUNT, true, var);
            return Optional.of(OpPlus2.of(OpPlus.of(bitmapCountOp, distinctId), bitmapAggOpPlus));
        } else {
            Op bitmapToArrayOp = Apply.apply(type, FunctionSet.BITMAP_TO_ARRAY, true, var);
            Op arraySumOp = Apply.apply(type, FunctionSet.ARRAY_SUM, true, bitmapToArrayOp);
            return Optional.of(OpPlus2.of(OpPlus.of(arraySumOp, distinctId), bitmapAggOpPlus));
        }
    }

    public static boolean isNdv(GenericColumn column) {
        return isFun(column, FunctionSet.NDV);
    }

    // APPROX_COUNT_DISTINCT is alias of NDV
    public static boolean isApproxCountDistinct(GenericColumn column) {
        return isFun(column, FunctionSet.APPROX_COUNT_DISTINCT);
    }

    public static boolean isHllRaw(GenericColumn column) {
        return isFun(column, FunctionSet.HLL_RAW);
    }

    public static boolean isHllUnion(GenericColumn column) {
        return isFun(column, FunctionSet.HLL_UNION);
    }

    // HLL_RAW_AGG is alias of HLL_UNION
    public static boolean isHllRawAgg(GenericColumn column) {
        return isFun(column, FunctionSet.HLL_RAW_AGG);
    }

    public static boolean isHllUnionAgg(GenericColumn column) {
        return isFun(column, FunctionSet.HLL_UNION_AGG);
    }

    public static boolean isHllMerge(GenericColumn column) {
        return isHllRaw(column) || isHllRawAgg(column) || isHllUnion(column);
    }

    public static boolean isHllFinal(GenericColumn column) {
        return isNdv(column) || isApproxCountDistinct(column) || isHllUnionAgg(column);
    }

    public static boolean isBitmapUnion(GenericColumn column) {
        return isFun(column, FunctionSet.BITMAP_UNION);
    }

    public static boolean isBitmapAgg(GenericColumn column) {
        return isFun(column, FunctionSet.BITMAP_AGG);
    }

    public static boolean isBitmapUnionInt(GenericColumn column) {
        return isFun(column, FunctionSet.BITMAP_UNION_INT);
    }

    public static boolean isBitmapUnionCount(GenericColumn column) {
        return isFun(column, FunctionSet.BITMAP_UNION_COUNT);
    }

    public static boolean isBitmapMerge(GenericColumn column) {
        return isBitmapAgg(column) || isBitmapUnion(column);
    }

    public static boolean isBitmapFinal(GenericColumn column) {
        return isBitmapUnionInt(column) || isBitmapUnionCount(column);
    }

    // BITMAP_UNION_INT = BITMAP_COUNT . BITMAP_AGG
    // BITMAP_UNION_COUNT = BITMAP_COUNT . BITMAP_UNION
    public static Optional<OpPlus2> rewriteBitmap(OpPlus bitmapPlus,
                                                  Supplier<Integer> idGen,
                                                  Map<StrictOp, Integer> alreadyExists) {
        int bitmapId = bitmapPlus.getId();
        Op bitmap = bitmapPlus.getOp();
        Op arg;
        if (bitmap.isFun(FunctionSet.BITMAP_UNION_INT)) {
            arg = Apply.apply(Type.BITMAP, FunctionSet.BITMAP_AGG, true, bitmap.getArgs());
        } else if (bitmap.isFun(FunctionSet.BITMAP_UNION_COUNT)) {
            arg = Apply.apply(Type.BITMAP, FunctionSet.BITMAP_UNION, true, bitmap.getArgs());
        } else {
            return Optional.empty();
        }

        EitherOr<OpPlus> argPlus = Optional.ofNullable(alreadyExists.get(arg.strict()))
                .map(id -> EitherOr.either(OpPlus.of(arg, id)))
                .orElseGet(() -> EitherOr.or(OpPlus.of(arg, idGen.get())));

        Op var = argPlus.get().toVar();
        Op bitmapCard = Apply.apply(bitmap.getType(), FunctionSet.BITMAP_COUNT, true, var);
        return Optional.of(OpPlus2.of(OpPlus.of(bitmapCard, bitmapId), argPlus));
    }

    public static boolean isPercentileUnion(GenericColumn column) {
        return isFun(column, FunctionSet.PERCENTILE_UNION);
    }

    public static boolean isPercentileApprox(GenericColumn column) {
        return isFun(column, FunctionSet.PERCENTILE_APPROX);
    }

    // PERCENTILE_APPROX a r = PERCENTILE_APPROX_RAW . (PERCENTILE_APPROX . PERCENTILE_HASH a) r
    public static Optional<OpPlus2> rewritePercentile(OpPlus percentilePlus, Supplier<Integer> idGen,
                                                      Map<StrictOp, Integer> alreadyExists) {
        int percentileId = percentilePlus.getId();
        Op percentile = percentilePlus.getOp();
        Preconditions.checkArgument(percentile.isFun(FunctionSet.PERCENTILE_APPROX));
        Op arg = percentile.arg(0);
        Op rate = percentile.arg(1);
        Op percentileHash = Op.apply(Type.PERCENTILE, FunctionSet.PERCENTILE_HASH, true, arg);
        Op percentileUnion = Op.apply(Type.PERCENTILE, FunctionSet.PERCENTILE_UNION, true, percentileHash);
        EitherOr<OpPlus> argPlus = Optional.ofNullable(alreadyExists.get(percentileUnion.strict()))
                .map(id -> EitherOr.either(OpPlus.of(percentileUnion, id)))
                .orElseGet(() -> EitherOr.or(OpPlus.of(percentileUnion, idGen.get())));
        Var var = argPlus.get().toVar();
        Op percentileApproxRaw = Op.apply(percentile.getType(), FunctionSet.PERCENTILE_APPROX_RAW, true, var, rate);
        return Optional.of(OpPlus2.of(OpPlus.of(percentileApproxRaw, percentileId), argPlus));
    }

    public static boolean isHll(GenericColumn column) {
        return isNdv(column) ||
                isApproxCountDistinct(column) ||
                isHllRaw(column) ||
                isHllUnion(column) ||
                isHllRawAgg(column) ||
                isHllUnionAgg(column);
    }

    public static boolean isDistinct(GenericColumn column) {
        return getOp(column).map(Op::isDistinctAgg).orElse(false);
    }

    public static boolean isSum(GenericColumn column) {
        return isFun(column, FunctionSet.SUM);
    }

    public static boolean isCount(GenericColumn column) {
        return isFun(column, FunctionSet.COUNT);
    }

    public static boolean isSumOrCount(GenericColumn column) {
        return isCount(column) || isSum(column);
    }

    // AVG = SUM/COUNT
    public static Optional<OpPlus2> rewriteAvg(OpPlus avgPlus, Supplier<Integer> idGen,
                                               Map<StrictOp, Integer> alreadyExists) {
        Op avg = avgPlus.getOp();
        Integer avgId = avgPlus.getId();
        Preconditions.checkArgument(avg != null && avg.isFun(FunctionSet.AVG));

        Apply apply = avg.cast();
        Op sumOp = Op.apply(avg.arg(0).getType(), FunctionSet.SUM, true, apply.getArgs());
        Op countOp = Op.apply(Type.BIGINT, FunctionSet.COUNT, true, apply.getArgs());
        List<EitherOr<OpPlus>> argPlusList =
                Stream.of(sumOp, countOp)
                        .map(arg -> Optional.ofNullable(alreadyExists.get(arg.strict()))
                                .map(id -> EitherOr.either(OpPlus.of(arg, id)))
                                .orElseGet(() -> EitherOr.or(OpPlus.of(arg, idGen.get()))))
                        .collect(ImmutableList.toImmutableList());
        List<Op> args = argPlusList.stream()
                .map(EitherOr::get)
                .map(OpPlus::toVar)
                .collect(ImmutableList.toImmutableList());

        Op newAvg = Op.apply(avg.getType(), FunctionSet.DIVIDE, true, args);
        return Optional.of(OpPlus2.of(OpPlus.of(newAvg, avgId), argPlusList));
    }

    // HLL_RAW: T->HLL
    // NDV/APPROX_COUNT_DISTINCT: T->INT = HLL_CARDINALITY . HLL_RAW
    // HLL_UNION/HLL_RAW_AGG: HLL->HLL
    // HLL_UNION_AGG: HLL->INT = HLL_CARDINALITY . HLL_UNION
    // It seems that NDV = HLL_CARDINALITY . HLL_UNION . HLL_HASH, however,
    //
    // Hash computation in update method of NDV is slightly different with HLL_HASH,
    // HLL_HASH only has string types, while NDV's hash function handle string types
    // and non-string types in different ways. so:
    // NDV(a) != HLL_UNION_AGG(HLL_HASH(a)), instead
    // NDV(CAST(a AS STRING))  = HLL_UNION_AGG(HLL_HASH(a)).
    // so HLL_RAW/NDV/APPROX_COUNT_DISTINCT is incompatible with HLL_UNION/HLL_RAW_AGG/HLL_UNION_AGG
    public static Optional<OpPlus2> rewriteHll(OpPlus hllOp, Supplier<Integer> idGen,
                                               Map<StrictOp, Integer> existingOps) {
        Op op = hllOp.getOp();
        Op arg;
        if (op.isFun(FunctionSet.NDV) && op.isFun(FunctionSet.APPROX_COUNT_DISTINCT)) {
            arg = Op.apply(Type.HLL, FunctionSet.HLL_RAW, true, op.getArgs());
        } else if (op.isFun(FunctionSet.HLL_UNION_AGG)) {
            arg = Op.apply(Type.HLL, FunctionSet.HLL_UNION, true, op.getArgs());
        } else {
            return Optional.empty();
        }

        EitherOr<OpPlus> opPlus = Optional.ofNullable(existingOps.get(arg.strict()))
                .map(id -> EitherOr.either(OpPlus.of(arg, id)))
                .orElseGet(() -> EitherOr.or(OpPlus.of(arg, idGen.get())));

        Op var = opPlus.get().toVar();
        Op hllCardOp = Op.apply(hllOp.getOp().getType(), FunctionSet.HLL_CARDINALITY, true, var);

        OpPlus hllCardOpPlus = OpPlus.of(hllCardOp, hllOp.getId());
        return Optional.of(OpPlus2.of(hllCardOpPlus, opPlus));
    }

    public static ColumnRefSet eliminateTrivialIsNotNull(List<Op> conjuncts) {
        if (conjuncts.stream().noneMatch(Op::isVarIsNotNull)) {
            return new ColumnRefSet();
        }
        Map<Boolean, List<Op>> conjGroups = conjuncts.stream().collect(Collectors.partitioningBy(Op::isVarIsNotNull));
        List<Op> isNotNullConjuncts = conjGroups.get(true);
        Set<Integer> ids = isNotNullConjuncts.stream().map(op -> op.unmodified().getId()).collect(Collectors.toSet());
        List<Op> otherConjuncts = conjGroups.get(false).stream().map(Op::eliminateInRange).collect(Collectors.toList());
        Op conjunct = Op.and(otherConjuncts);
        ColumnRefSet eraseIds = new ColumnRefSet();
        for (Integer id : ids) {
            ColumnRefSet nullIds = new ColumnRefSet(id);
            if (PartiallyApplyNullsEval.isFalseOrNull(conjunct, nullIds)) {
                eraseIds.union(id);
            }
        }
        return eraseIds;
    }

    public static Optional<String> getFnName(GenericColumn column) {
        return getOp(column).map(op -> op.cast(Apply.class))
                .flatMap(Function.identity())
                .map(apply -> apply.getKind().toString());
    }

    public static String mustGetFnName(GenericColumn column) {
        return Objects.requireNonNull(getFnName(column).orElse(null));
    }

    private static Optional<Op> substImpl(Op op, Map<Integer, Op> opMap) {
        if (op.isVal()) {
            return Optional.empty();
        }
        if (op.isVar()) {
            return Optional.ofNullable(opMap.get(op.getId()));
        }
        List<Pair<Op, Optional<Op>>> argPairs = op.getArgs().stream()
                .map(arg -> Pair.create(arg, substImpl(arg, opMap)))
                .collect(Collectors.toList());
        if (argPairs.stream().noneMatch(p -> p.second.isPresent())) {
            return Optional.empty();
        } else {
            List<Op> newArgs = argPairs.stream()
                    .map(p -> p.second.orElse(p.first))
                    .collect(ImmutableList.toImmutableList());
            Apply oldApply = op.cast();
            Op newOp = Op.apply(oldApply.getType(), oldApply.getKind(), oldApply.isOrdered(), newArgs);
            return Optional.of(newOp);
        }
    }

    public static Optional<Op> subst(Op op, Map<Integer, Op> opMap) {
        ColumnRefSet columnIds = ColumnRefSet.createByIds(opMap.keySet());
        if (!columnIds.isIntersect(op.getIds())) {
            return Optional.empty();
        } else {
            return substImpl(op, opMap);
        }
    }

    private static GenericColumn subst(GenericColumn column,
                                       Map<Integer, Op> idToOpMap) {
        if (column.isOriginal()) {
            return column;
        } else {
            DerivedColumn derivedColumn = column.cast();
            Op oldOp = derivedColumn.getOp();
            Op newOp = subst(derivedColumn.getOp(), idToOpMap).orElse(oldOp);
            return GenericColumn.derived(newOp);
        }
    }

    public static TieredMap<Integer, GenericColumn> subst(List<GenericColumn> columns,
                                                          Map<Integer, Op> idToOpMap,
                                                          ColumnRefToIdConverter idConverter) {
        return columns.stream().collect(TieredMap.toMap(
                e -> idConverter.nextId(),
                e -> subst(e, idToOpMap)
        ));
    }

    public static Function<Op, Op> toSubstitute(TieredMap<Integer, Op> opMap) {
        return op -> subst(op, opMap).orElse(op);
    }

    public static TieredMap<Integer, GenericColumn> columnize(Collection<Pair<Integer, Op>> ops) {
        return ops.stream().collect(TieredMap.toMap(p -> p.first, p -> GenericColumn.derived(p.second)));
    }

    public static TieredMap<Integer, GenericColumn> columnize(TieredMap<Integer, Op> ops) {
        return ops.entrySet().stream().collect(TieredMap.toMap(
                Map.Entry::getKey,
                e -> GenericColumn.derived(e.getValue())));
    }

    private static Optional<Op> unfoldImpl(Op op, TieredMap<Integer, GenericColumn> underlyingColumns) {
        if (op.isVal()) {
            return Optional.empty();
        } else if (op.isVar()) {
            // var must be present in underlying columns
            return Optional.of(underlyingColumns.get(op.getId()).getNormalizedOp());
        } else if (op.getIds().isEmpty()) {
            return Optional.empty();
        } else {
            Apply oldOp = op.cast();
            List<Op> newArgs = op.getArgs().stream()
                    .map(arg -> unfoldImpl(arg, underlyingColumns).orElse(arg))
                    .collect(Collectors.toList());
            return Optional.of(Apply.apply(oldOp.getType(), oldOp.getKind(), oldOp.isOrdered(), newArgs));
        }
    }

    private static Op unfold(Op op, TieredMap<Integer, GenericColumn> underlyingColumns) {
        return unfoldImpl(op, underlyingColumns).orElse(op.clone());
    }

    public static Op unfoldOp(Op op, TieredMap<Integer, GenericColumn> underlyingColumns) {
        op.setNorm(unfold(op, underlyingColumns));
        return op;
    }

    public static GenericColumn unfoldDerivedColumn(DerivedColumn derivedColumn,
                                                    TieredMap<Integer, GenericColumn> underlyingColumns) {
        Op newOp = OpUtil.unfold(derivedColumn.getExpr().getOp(), underlyingColumns);
        GenericColumn normColumn = GenericColumn.derived(newOp);
        derivedColumn.setNorm(normColumn);
        return derivedColumn;
    }

    public static Op columnToOp(int id, GenericColumn column) {
        if (column.isOriginal()) {
            return Apply.var(column.getType(), id);
        } else {
            return column.getOp();
        }
    }

    public static List<TieredList<Op>> seekForIdInOp(Op op, Integer id) {
        if (!op.getIds().contains(id)) {
            return Collections.emptyList();
        } else {
            TieredList<Op> path = TieredList.genesis();
            return seekForIdInOpImpl(op, id, path);
        }
    }

    private static List<TieredList<Op>> seekForIdInOpImpl(Op op, Integer id, TieredList<Op> path) {
        if (!op.getIds().contains(id)) {
            return Collections.emptyList();
        }
        TieredList<Op> newPath = path.concatOne(op);
        if (op.isVar()) {
            return Collections.singletonList(newPath);
        }
        return op.getArgs().stream()
                .filter(arg -> arg.getIds().contains(id))
                .map(arg -> seekForIdInOpImpl(arg, id, newPath))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }
}
