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

package com.starrocks.sql.automv.pieces;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.generator.ColumnRefToIdConverter;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.pn.ReverseTranscriptase;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class PlanPieceBuilder extends OptExpressionVisitor<PlanPiece, PlanPieceBuildContext> {

    private static final PlanPieceBuilder INSTANCE = new PlanPieceBuilder();

    private PlanPieceBuilder() {
    }

    public static Function<ScalarOperator, Op> toOpConverter(final ColumnRefToIdConverter idConverter,
                                                             final Map<Integer, GenericColumn> inputColumns) {
        return scalarOperator -> ReverseTranscriptase.reverseTranscript(scalarOperator, idConverter, inputColumns);
    }

    private static GenericColumn convScalarOperator(final ScalarOperator scalarOperator,
                                                    final ColumnRefToIdConverter idConverter,
                                                    final Map<Integer, GenericColumn> inputColumns) {
        Op op = toOpConverter(idConverter, inputColumns).apply(scalarOperator);
        if (op.isVar() && op.getColumn().isOriginal()) {
            return op.getColumn();
        } else {
            return GenericColumn.derived(op);
        }
    }

    private static Function<ScalarOperator, GenericColumn> toColumnConverter(
            final ColumnRefToIdConverter idConverter,
            final Map<Integer, GenericColumn> inputColumns) {
        return scalarOperator -> convScalarOperator(scalarOperator, idConverter, inputColumns);
    }

    private static Function<Column, GenericColumn> toOriginalColumnConverter(TableName fqTableName) {
        return column -> GenericColumn.original(fqTableName, column);
    }

    public static PlanPiece createPlanPiece(OptExpression optExpression, ColumnRefToIdConverter idConverter,
                                            Map<String, FQTable> fqTableMap) {
        PlanPieceBuildContext context = PlanPieceBuildContext.of(idConverter, fqTableMap, Collections.emptyList());
        return PlanPieceBuilder.INSTANCE.build(optExpression, context);
    }

    private TieredMap<Integer, GenericColumn> convAggCalls(
            final Collection<Pair<ColumnRefOperator, CallOperator>> aggCalls,
            final ColumnRefToIdConverter idConverter,
            final Map<Integer, GenericColumn> inputColumns) {
        return aggCalls.stream().collect(TieredMap.toMap(
                p -> idConverter.getId(p.first),
                p -> toColumnConverter(idConverter, inputColumns).apply(p.second)));
    }

    private TieredMap<Integer, GenericColumn> convColumnRefs(
            final Collection<ColumnRefOperator> colRefs,
            final ColumnRefToIdConverter idConverter,
            final Map<Integer, GenericColumn> inputColumns) {
        return colRefs.stream().collect(TieredMap.toMap(
                idConverter::getId,
                c -> toColumnConverter(idConverter, inputColumns).apply(c)));
    }

    private TieredMap<Integer, GenericColumn> convColumnRefMap(
            final Map<ColumnRefOperator, ? extends ScalarOperator> columnRefMap,
            final ColumnRefToIdConverter idConverter,
            final Map<Integer, GenericColumn> inputColumns) {

        return columnRefMap.entrySet()
                .stream()
                .filter(e -> !(e.getValue().isColumnRef() && e.getValue().equals(e.getKey())))
                .collect(TieredMap.toMap(
                        e -> idConverter.getId(e.getKey()),
                        e -> toColumnConverter(idConverter, inputColumns).apply(e.getValue())));
    }

    private TieredMap<Integer, GenericColumn> handleProject(Operator op,
                                                            Map<Integer, GenericColumn> originColumns,
                                                            ColumnRefToIdConverter idConverter) {
        return Optional.ofNullable(op.getProjection())
                .map(Projection::getColumnRefMap)
                .map(colRefMap -> convColumnRefMap(colRefMap, idConverter, originColumns))
                .orElse(TieredMap.genesis());
    }

    private TieredList<Op> handlePredicate(ScalarOperator predicate,
                                           Map<Integer, GenericColumn> inputColumns,
                                           ColumnRefToIdConverter idConverter) {
        if (predicate == null) {
            return TieredList.genesis();
        } else {
            return toOpConverter(idConverter, inputColumns)
                    .apply(predicate).conjuncts().stream()
                    .collect(TieredList.toList());
        }
    }

    @Override
    public PlanPiece visitLogicalTableScan(OptExpression optExpression, PlanPieceBuildContext args) {
        LogicalScanOperator tableScan = optExpression.getOp().cast();
        ColumnRefToIdConverter idConverter = args.getIdConverter();
        FQTable fqTable = args.getFqTableMap().get(tableScan.getTable().getUUID());
        Function<Column, GenericColumn> columnConverter = toOriginalColumnConverter(fqTable.getFqTableName());
        TieredMap<Integer, GenericColumn> originColumns =
                tableScan.getColRefToColumnMetaMap().entrySet()
                        .stream()
                        .collect(TieredMap.toMap(e -> idConverter.getId(e.getKey()),
                                e -> columnConverter.apply(e.getValue())));

        TieredMap<Integer, GenericColumn> derivedColumns =
                handleProject(optExpression.getOp(), originColumns, idConverter);
        TieredMap<Integer, GenericColumn> columns = originColumns.merge(derivedColumns);

        TieredList<Op> conjuncts = handlePredicate(optExpression.getOp().getPredicate(), columns, idConverter);
        return TablePiece.newBuilder()
                .setTable(args.getFqTableMap().get(tableScan.getTable().getUUID()))
                .setColumns(columns)
                .setCommonState(args.getCommonState())
                .setAuxState(new PieceAuxState())
                .setConjuncts(conjuncts).build();
    }

    PlanPiece handleProjectAndFilter(LogicalOperator operator, PlanPiece inputPiece,
                                     ColumnRefToIdConverter idConverter) {
        TieredMap<Integer, GenericColumn> columns = handleProject(operator, inputPiece.getColumns(), idConverter);
        TieredList<Op> conjuncts = handlePredicate(operator.getPredicate(), inputPiece.getColumns(), idConverter);
        columns = inputPiece.getColumns().merge(columns);
        conjuncts = inputPiece.getConjuncts().concat(conjuncts);

        return inputPiece.builder()
                .setColumns(columns)
                .setConjuncts(conjuncts)
                .build();
    }

    @Override
    public PlanPiece visitLogicalProject(OptExpression optExpression, PlanPieceBuildContext args) {
        return handleProjectAndFilter(optExpression.getOp().cast(), args.arg0(), args.getIdConverter());
    }

    @Override
    public PlanPiece visitLogicalFilter(OptExpression optExpression, PlanPieceBuildContext args) {
        return handleProjectAndFilter(optExpression.getOp().cast(), args.arg0(), args.getIdConverter());
    }

    private Pair<JoinOperator, List<PlanPiece>> applyCommutative(LogicalJoinOperator join, PlanPiece lhs,
                                                                 PlanPiece rhs) {
        JoinOperator joinType = join.getJoinType();
        if (joinType.isRightOuterJoin()) {
            return Pair.create(JoinOperator.LEFT_OUTER_JOIN, ImmutableList.of(rhs, lhs));
        } else if (joinType.isRightAntiJoin()) {
            return Pair.create(JoinOperator.LEFT_ANTI_JOIN, ImmutableList.of(rhs, lhs));
        } else if (joinType.isRightSemiJoin()) {
            return Pair.create(JoinOperator.LEFT_SEMI_JOIN, ImmutableList.of(rhs, lhs));
        } else {
            return Pair.create(joinType, ImmutableList.of(lhs, rhs));
        }
    }

    private PlanPiece createStarJoinImpl(
            PlanPiece centre, List<StarJoinPiece.StarCorner> corners,
            List<Op> hoistConjuncts, TieredMap<Integer, GenericColumn> inputColumns) {
        return StarJoinPiece.newBuilder()
                .setCentre(centre)
                .setCorners(corners)
                .setColumns(inputColumns)
                .setConjuncts(TieredList.<Op>genesis().concat(hoistConjuncts))
                .setCommonState(centre.getCommonState())
                .setAuxState(new PieceAuxState())
                .build();
    }

    private PlanPiece createStarJoin(
            JoinOperator joinType,
            PlanPiece lhsPiece,
            PlanPiece rhsPiece,
            List<Op> eqConjuncts,
            List<Op> otherConjuncts,
            List<Op> hoistConjuncts) {

        StarJoinPiece.StarCorner corner = new StarJoinPiece.StarCorner(eqConjuncts, otherConjuncts, joinType, rhsPiece);
        List<StarJoinPiece.StarCorner> corners = Collections.singletonList(corner);
        TieredMap<Integer, GenericColumn> inputColumns;
        if (joinType.isLeftSemiAntiJoin()) {
            inputColumns = lhsPiece.getColumns();
        } else {
            inputColumns = lhsPiece.getColumns().merge(rhsPiece.getColumns());
        }
        return createStarJoinImpl(lhsPiece, corners, hoistConjuncts, inputColumns);
    }

    private PlanPiece mergeStarJoin(
            JoinOperator joinType,
            LogicalOperator join,
            PlanPiece lhsPiece,
            PlanPiece rhsPiece,
            List<Op> eqConjuncts,
            List<Op> otherConjuncts,
            List<Op> hoistConjuncts) {
        Preconditions.checkArgument(lhsPiece.isStarJoin());
        StarJoinPiece lhsJoinPiece = (StarJoinPiece) lhsPiece;
        PlanPiece centre = lhsJoinPiece.getCentre();
        List<StarJoinPiece.StarCorner> corners = lhsJoinPiece.getCorners();
        StarJoinPiece.StarCorner corner = new StarJoinPiece.StarCorner(eqConjuncts, otherConjuncts, joinType, rhsPiece);
        List<StarJoinPiece.StarCorner> newCorners = Lists.newArrayList(corners);
        newCorners.add(corner);
        TieredMap<Integer, GenericColumn> inputColumns;
        if (joinType.isLeftSemiAntiJoin()) {
            inputColumns = lhsJoinPiece.getColumns();
        } else {
            inputColumns = lhsJoinPiece.getColumns().merge(rhsPiece.getColumns());
        }
        return createStarJoinImpl(centre, newCorners, hoistConjuncts, inputColumns);
    }

    private PlanPiece createOrMergeStarJoin(JoinOperator joinType, LogicalOperator join, StarJoinPiece lhsPiece,
                                            PlanPiece rhsPiece, List<Op> eqConjuncts, List<Op> otherConjuncts,
                                            List<Op> hoistConjuncts) {
        PlanPiece centre = lhsPiece.getCentre();
        ColumnRefSet colRefSet = new ColumnRefSet();
        colRefSet.union(ColumnRefSet.createByIds(centre.getColumns().keySet()));
        colRefSet.union(ColumnRefSet.createByIds(rhsPiece.getColumns().keySet()));
        ColumnRefSet joinColRefSet = new ColumnRefSet();
        eqConjuncts.forEach(op -> joinColRefSet.union(op.getIds()));
        otherConjuncts.forEach(op -> joinColRefSet.union(op.getIds()));
        boolean canMerge = colRefSet.containsAll(joinColRefSet);
        if (canMerge) {
            return mergeStarJoin(joinType, join, lhsPiece, rhsPiece, eqConjuncts, otherConjuncts,
                    hoistConjuncts);
        } else {
            return createStarJoin(joinType, lhsPiece, rhsPiece, eqConjuncts, otherConjuncts,
                    hoistConjuncts);
        }
    }

    @Override
    public PlanPiece visitLogicalJoin(OptExpression optExpression, PlanPieceBuildContext args) {
        LogicalJoinOperator join = optExpression.getOp().cast();
        Pair<JoinOperator, List<PlanPiece>> typeAndArgs = applyCommutative(join, args.arg0(), args.arg1());
        JoinOperator joinType = typeAndArgs.first;
        PlanPiece lhsPiece = typeAndArgs.second.get(0);
        PlanPiece rhsPiece = typeAndArgs.second.get(1);
        TieredMap<Integer, GenericColumn> inputColumns =
                rhsPiece.getColumns().merge(lhsPiece.getColumns());

        ColumnRefToIdConverter idConverter = args.getIdConverter();
        // Predicates hoisting
        TieredList<Op> onConjuncts = handlePredicate(join.getOnPredicate(), inputColumns, idConverter);
        Map<Boolean, List<Op>> conjGroups = onConjuncts.stream().collect(Collectors.partitioningBy(Op::isVEV));
        ColumnRefSet lhsIds = ColumnRefSet.createByIds(lhsPiece.getColumns().keySet());
        ColumnRefSet rhsIds = ColumnRefSet.createByIds(rhsPiece.getColumns().keySet());
        Function<Op, Op> vevSwapper = Op.toVEVSwapper(lhsIds, rhsIds);
        List<Op> eqConjuncts = conjGroups.get(true).stream()
                .map(vevSwapper)
                .collect(Collectors.toList());

        List<Op> otherConjuncts = conjGroups.get(false);

        List<HoistTriple> hoistTriples =
                HoistTriple.prepare(eqConjuncts, otherConjuncts, lhsPiece.getConjuncts(), rhsPiece.getConjuncts());

        List<Op> hoistConjuncts = Lists.newArrayList();
        List<Op> newOnConjuncts = Lists.newArrayList();
        List<Op> lhsConjuncts = Lists.newArrayList();
        List<Op> rhsConjuncts = Lists.newArrayList();

        for (HoistTriple triple : hoistTriples) {
            HoistFunction.get(joinType).apply(triple, lhsConjuncts, rhsConjuncts, newOnConjuncts, hoistConjuncts);
        }

        // Trivial IS_NOT_NULL elimination
        List<Op> allConjuncts = Lists.newArrayList(Iterables.concat(eqConjuncts, hoistConjuncts, newOnConjuncts));
        ColumnRefSet eraseIds = OpUtil.eliminateTrivialIsNotNull(allConjuncts);

        Predicate<Op> shouldRetain = op -> !op.isVarIsNotNull() || !eraseIds.contains(op.unmodified().getId());
        newOnConjuncts = newOnConjuncts.stream().filter(shouldRetain).collect(Collectors.toList());
        hoistConjuncts = hoistConjuncts.stream().filter(shouldRetain).collect(Collectors.toList());

        lhsPiece = lhsPiece.setConjuncts(TieredList.<Op>genesis().concat(lhsConjuncts));
        rhsPiece = rhsPiece.setConjuncts(TieredList.<Op>genesis().concat(rhsConjuncts));

        // create/merge StarJoin Piece
        PlanPiece starJoinPiece;
        if (joinType.isFullOuterJoin() ||
                joinType.isLeftOuterJoin() ||
                (!lhsPiece.isStarJoin() && !rhsPiece.isStarJoin()) ||
                (!lhsPiece.isStarJoin() && joinType.isLeftSemiAntiJoin())) {
            starJoinPiece = createStarJoin(joinType, lhsPiece, rhsPiece, eqConjuncts, newOnConjuncts, hoistConjuncts);
        } else if (lhsPiece.isStarJoin()) {
            StarJoinPiece joinPiece = (StarJoinPiece) lhsPiece;
            starJoinPiece = createOrMergeStarJoin(joinType, join, joinPiece, rhsPiece, eqConjuncts, newOnConjuncts,
                    hoistConjuncts);
        } else if (rhsPiece.isStarJoin() && join.isInnerOrCrossJoin()) {
            StarJoinPiece joinPiece = (StarJoinPiece) rhsPiece;
            starJoinPiece = createOrMergeStarJoin(joinType, join, joinPiece, lhsPiece, eqConjuncts, newOnConjuncts,
                    hoistConjuncts);
        } else {
            starJoinPiece = createStarJoin(joinType, lhsPiece, rhsPiece, eqConjuncts, newOnConjuncts, hoistConjuncts);
        }
        return handleProjectAndFilter(join, starJoinPiece, idConverter);
    }

    @Override
    public PlanPiece visitLogicalAggregate(OptExpression optExpression, PlanPieceBuildContext args) {
        LogicalAggregationOperator agg = optExpression.getOp().cast();
        PlanPiece flatTable = args.arg0();
        ColumnRefToIdConverter idConverter = args.getIdConverter();
        ColumnRefSet groupKeyColRefSet = new ColumnRefSet(agg.getGroupingKeys());
        TieredMap<Integer, GenericColumn> dimensions =
                convColumnRefs(agg.getGroupingKeys(), idConverter, flatTable.getColumns());

        Map<Boolean, List<Op>> conjGroups = flatTable.getConjuncts().stream()
                .collect(Collectors.partitioningBy(op -> groupKeyColRefSet.containsAll(op.getIds())));
        List<Op> hoistConjuncts = conjGroups.get(true);
        List<Op> nonhoistConjuncts = conjGroups.get(false);
        Set<Integer> rollupColumnRefs = nonhoistConjuncts.stream().flatMap(op -> op.getIdSet().stream())
                .filter(colRef -> !groupKeyColRefSet.contains(colRef)).collect(Collectors.toSet());

        TieredMap<Integer, GenericColumn> rollupDimension = flatTable.getColumns().entrySet()
                .stream().filter(e -> rollupColumnRefs.contains(e.getKey()))
                .collect(TieredMap.toMap());

        Map<Boolean, List<Pair<ColumnRefOperator, CallOperator>>> aggGroups =
                agg.getAggregations().entrySet().stream().map(e -> Pair.create(e.getKey(), e.getValue()))
                        .collect(Collectors.partitioningBy(p -> p.second.isDistinct()));

        TieredMap<Integer, GenericColumn> distinctMetrics =
                convAggCalls(aggGroups.get(true), idConverter, flatTable.getColumns());

        TieredMap<Integer, GenericColumn> metrics =
                convAggCalls(aggGroups.get(false), idConverter, flatTable.getColumns());

        flatTable = flatTable.setConjuncts(TieredList.<Op>genesis().concat(nonhoistConjuncts));

        AggregatePiece aggPiece = AggregatePiece.newBuilder()
                .setFlatTable(flatTable)
                .setDimensions(dimensions)
                .setRollupDimensions(rollupDimension)
                .setMetrics(metrics)
                .setDistinctMetrics(distinctMetrics)
                .setConjuncts(TieredList.genesis())
                .setCommonState(args.getCommonState())
                .setAuxState(new PieceAuxState())
                .build().cast();

        return handleProjectAndFilter(agg, aggPiece, idConverter);
    }

    private PlanPiece build(OptExpression optExpression, PlanPieceBuildContext context) {
        List<PlanPiece> inputPieces =
                optExpression.getInputs().stream().map(child -> build(child, context)).collect(Collectors.toList());
        PlanPieceBuildContext newContext = context.newContextWithPieces(inputPieces);
        return optExpression.getOp().accept(this, optExpression, newContext);
    }
}
