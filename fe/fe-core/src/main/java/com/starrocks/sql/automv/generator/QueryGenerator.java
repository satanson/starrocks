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

package com.starrocks.sql.automv.generator;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.DerivedColumn;
import com.starrocks.sql.automv.pieces.GenericColumn;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.PlanPieceVisitor;
import com.starrocks.sql.automv.pieces.StarJoinPiece;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class QueryGenerator {

    public static QueryGenerateResult generate(PlanPiece planPiece, PrettyPrinter traceLog) {
        Preconditions.checkArgument(planPiece.isAggregate());
        Visitor visitor = new Visitor(traceLog);
        return visitor.generate(planPiece).setTraceLog(visitor.getTraceLog().orElse(null));
    }

    public static QueryGenerateResult generate(PlanPiece planPiece) {
        return generate(planPiece, null);
    }

    private static final class Visitor extends PlanPieceVisitor<QueryGenerateResult, QueryGenerateContext> {
        private final Optional<PrettyPrinter> optTraceLog;
        private final AliasGenerator aliasGenerator;

        private Visitor(@Nullable PrettyPrinter traceLog) {
            this.optTraceLog = Optional.ofNullable(traceLog);
            this.aliasGenerator = AliasGenerator.getDefaultAliasGenerator();
        }

        @Override
        public QueryGenerateResult visitAggregate(AggregatePiece aggPiece, QueryGenerateContext context) {

            QueryGenerateResult result = context.getInputResults().get(0);
            String newTableAlias = aliasGenerator.nextAliasIfTableNameAbsent(null);
            QueryGenerateResult newResult = synthesizeSubquery(aggPiece, newTableAlias, result, context);
            Preconditions.checkArgument(aggPiece.getRollupDimensions().isEmpty());

            List<Integer> groupByColumns = result.getOrderedDimensions().stream()
                    .map(p -> p.first).collect(Collectors.toList());
            if (groupByColumns.isEmpty()) {
                groupByColumns = Lists.newArrayList(aggPiece.getDimensions().keySet());
            }

            List<String> groupByItems = groupByColumns.stream()
                    .map(result.getColumnAliases()::get)
                    .map(ColumnAlias::getQualifiedName)
                    .collect(Collectors.toList());

            if (!groupByColumns.isEmpty()) {
                PrettyPrinter subquery = newResult.getSubquery();
                subquery.newLine().add("GROUP BY").newLine();
                subquery.indentEnclose(() -> {
                    subquery.addItemsWithNlDel(", ", groupByItems);
                });
            }

            return newResult;
        }

        Function<Pair<Integer, GenericColumn>, Integer> getColumnWeightCalculator(
                List<Op> conjuncts, ColumnRefSet dimensionIds) {
            List<ColumnRefSet> columnRefs = conjuncts.stream()
                    .map(Op::getIds)
                    .collect(Collectors.toList());

            return p -> {
                int g = dimensionIds.contains(p.first) && p.second.getType().canDistributedBy() ? 1 : 0;
                int h = (int) columnRefs.stream().filter(colRef -> colRef.contains(p.first)).count();
                int w = p.second.getType().getPrimitiveType().isVariableLengthType() ? 1 : 2;
                return -(g * (10 * h + w));
            };
        }

        private Pair<List<Pair<Integer, GenericColumn>>, List<Pair<Integer, GenericColumn>>> sortOutputColumnsIfTopAgg(
                List<Pair<Integer, GenericColumn>> outputColumns, PlanPiece piece) {
            if (!(piece.isTop() && piece.isAggregate())) {
                return Pair.create(outputColumns, Collections.emptyList());
            }
            AggregatePiece aggPiece = piece.cast();
            ColumnRefSet dimensionIds = ColumnRefSet.createByIds(aggPiece.getDimensions().keySet());
            Function<Pair<Integer, GenericColumn>, Integer> calculator =
                    getColumnWeightCalculator(aggPiece.getFlatTable().getConjuncts(), dimensionIds);

            Map<Boolean, TieredList<Pair<Integer, GenericColumn>>> columnGroups = outputColumns.stream()
                    .collect(Collectors.partitioningBy(p -> dimensionIds.contains(p.first),
                            TieredList.<Pair<Integer, GenericColumn>>toList()));

            TieredList<Pair<Integer, GenericColumn>> orderedDimensions = columnGroups.get(true).stream()
                    .map(p -> Pair.create(p, calculator.apply(p)))
                    .sorted(Pair.comparingBySecond())
                    .map(p -> p.first)
                    .collect(TieredList.toList());
            return Pair.create(orderedDimensions.concat(columnGroups.get(false)), orderedDimensions);
        }

        private QueryGenerateResult synthesizeSubquery(
                PlanPiece planPiece,
                @Nullable String newTableAlias,
                QueryGenerateResult inputResult,
                QueryGenerateContext context) {
            TieredMap<Integer, ColumnAlias> columnAliases = inputResult.getColumnAliases();
            Function<Op, String> opToSql = OpUtil.toOpToSqlConverter(columnAliases);
            TieredMap.Builder<Integer, ColumnAlias> newColumnAliasesBuilder = TieredMap.newGenesisTier();
            List<String> selectItems = Lists.newArrayList();

            List<Pair<Integer, GenericColumn>> outputColumns = context.getOutputColumns();
            Pair<List<Pair<Integer, GenericColumn>>, List<Pair<Integer, GenericColumn>>>
                    outputColumnsAndDimensions = sortOutputColumnsIfTopAgg(outputColumns, planPiece);

            outputColumns = outputColumnsAndDimensions.first;
            List<Pair<Integer, GenericColumn>> orderedDimensions = outputColumnsAndDimensions.second;

            boolean allOriginal = outputColumns.stream().allMatch(p -> p.second.isOriginal());
            boolean hasNameCollision = allOriginal && outputColumns.stream()
                    .map(p -> Objects.requireNonNull(columnAliases.get(p.first)))
                    .map(ColumnAlias::getName).collect(Collectors.toSet()).size() < outputColumns.size();

            if (planPiece.isTop() || !allOriginal || hasNameCollision || !planPiece.getConjuncts().isEmpty()) {
                newTableAlias = aliasGenerator.nextAliasIfTableNameAbsent(null);
            }

            final String finalNewTableAlias = newTableAlias;
            Set<String> nameCollision = Sets.newHashSet();
            outputColumns.forEach(p -> {
                if (p.second.isOriginal()) {
                    ColumnAlias columnAlias = Objects.requireNonNull(columnAliases.get(p.first));
                    String name = columnAlias.getName();
                    if (nameCollision.contains(name)) {
                        ColumnAlias newColumnAlias = aliasGenerator.nextAliasIfColumnNameAbsent(null);
                        Preconditions.checkArgument(!nameCollision.contains(newColumnAlias.getName()));
                        selectItems.add(String.format("%s AS %s",
                                columnAlias.getQualifiedName(), newColumnAlias.getName()));
                        columnAlias = newColumnAlias;
                    } else {
                        selectItems.add(columnAlias.getQualifiedName());
                        columnAlias = columnAlias.rename(finalNewTableAlias, null);
                    }
                    nameCollision.add(columnAlias.getName());
                    newColumnAliasesBuilder.put(p.first, columnAlias);
                } else {
                    DerivedColumn derived = p.second.cast();
                    ColumnAlias columnAlias = aliasGenerator.nextAliasIfColumnNameAbsent(null);
                    String sql = opToSql.apply(derived.getExpr().getOp());
                    selectItems.add(String.format("(%s) AS %s", sql, columnAlias.getName()));
                    newColumnAliasesBuilder.put(p.first, columnAlias);
                }
            });

            TieredMap<Integer, ColumnAlias> newColumnAliases = newColumnAliasesBuilder.build();
            Optional<List<String>> whereConjuncts = OpUtil.conjunctsToSql(planPiece.getConjuncts(), opToSql);
            final PrettyPrinter newSubquery = new PrettyPrinter();
            if (finalNewTableAlias != null) {
                newSubquery.add("SELECT").newLine();
                newSubquery.indentEnclose(() -> {
                    newSubquery.addItemsWithNlDel(",", selectItems);
                });
                newSubquery.newLine().add("FROM").newLine();
                newSubquery.indentEnclose(() -> {
                    newSubquery.addSuperStepWithIndent(inputResult.toSql());
                });
                if (whereConjuncts.isPresent()) {
                    newSubquery.newLine().add("WHERE").newLine();
                    newSubquery.indentEnclose(() -> {
                        newSubquery.addItemsWithNlDel("AND ", whereConjuncts.get());
                    });
                }
                return QueryGenerateResult.of(newSubquery, newTableAlias, newColumnAliases)
                        .setOrderedDimensions(orderedDimensions)
                        .setOrderedColumns(outputColumns);
            } else {
                return inputResult.updateColumnAliases(newColumnAliases)
                        .setOrderedDimensions(orderedDimensions)
                        .setOrderedColumns(outputColumns);
            }
        }

        @Override
        public QueryGenerateResult visitTable(TablePiece tablePiece, QueryGenerateContext context) {
            ColumnRefSet inputColumnIds = context.getInputColumnIds();
            List<Pair<Integer, GenericColumn>> tableOutputColumns =
                    tablePiece.getOutputColumns(inputColumnIds);
            Preconditions.checkArgument(tableOutputColumns.stream().allMatch(p -> p.second.isOriginal()));

            String tableName = tablePiece.getTableName();
            String tableAlias = aliasGenerator.nextAliasIfTableNameAbsent(tableName);

            TieredMap<Integer, ColumnAlias> columnAliases =
                    tableOutputColumns.stream().collect(
                            TieredMap.toMap(
                                    p -> p.first,
                                    p -> aliasGenerator
                                            .nextAliasIfColumnNameAbsent(p.second.getColumnName())
                                            .rename(tableName, null)));

            boolean noDerivedColumn = context.getOutputColumns().stream().noneMatch(p -> p.second.isDerived());
            boolean noWhereClause = tablePiece.getConjuncts().isEmpty();
            boolean tableAliasNotNeed = noWhereClause && noDerivedColumn && tableAlias.equals(tableName);

            final String newTableAlias = tableAliasNotNeed ? null : tableAlias;
            PrettyPrinter subquery = new PrettyPrinter();
            subquery.add(tableName);
            QueryGenerateResult result = QueryGenerateResult.of(subquery, null, columnAliases);
            return synthesizeSubquery(tablePiece, newTableAlias, result, context);
        }

        private QueryGenerateResult synthesizeNotInSubquery(List<Op> eqConjuncts, boolean hasWhereClause,
                                                            QueryGenerateResult lhsResult,
                                                            QueryGenerateResult rhsResult) {
            Preconditions.checkArgument(eqConjuncts.stream().allMatch(op -> op.isEq() && op.arg(0).isVar()));
            List<Pair<Op, Op>> notInPair = eqConjuncts.stream()
                    .map(op -> Pair.create(op.arg(0), op.arg(1)))
                    .collect(Collectors.toList());
            List<Op> notInLhs = notInPair.stream().map(p -> p.first).collect(Collectors.toList());
            List<Op> notInRhs = notInPair.stream().map(p -> p.second).collect(Collectors.toList());

            TieredMap<Integer, ColumnAlias> columnAliases =
                    lhsResult.getColumnAliases().merge(rhsResult.getColumnAliases());
            Function<Op, String> opToSql = OpUtil.toOpToSqlConverter(columnAliases);

            List<String> notInLhsItems = notInLhs.stream().map(opToSql).collect(Collectors.toList());
            List<String> notInRhsItems = notInRhs.stream().map(opToSql).collect(Collectors.toList());
            PrettyPrinter notInClause = new PrettyPrinter();
            notInClause.add("(").addItems(", ", notInLhsItems).spaces(1).add("NOT IN (").newLine();
            notInClause.indentEnclose(() -> {
                notInClause.add("SELECT").spaces(1).addItems(", ", notInRhsItems).newLine();
                notInClause.add("FROM").spaces(1).newLine();
                notInClause.indentEnclose(() -> {
                    notInClause.addSuperStepWithIndent(rhsResult.toSql());
                });
            });
            notInClause.add(")").newLine();

            final PrettyPrinter newSubquery;
            String newTableAlias;
            TieredMap<Integer, ColumnAlias> newColumnAliases;

            // tableAlias is not null means lhsSubquery already is select statement.
            if (lhsResult.getTableAlias() != null) {
                newSubquery = lhsResult.getSubquery();
                newTableAlias = lhsResult.getTableAlias();
                newColumnAliases = lhsResult.getColumnAliases();
            } else {
                // tableAlias is null means lhsSubquery is bare table name or a join clause,
                // so we must create a select statement.
                PrettyPrinter newSubquery1 = new PrettyPrinter();

                List<String> selectItems = lhsResult.getColumnAliases().values()
                        .stream()
                        .map(ColumnAlias::getQualifiedName)
                        .collect(Collectors.toList());
                newSubquery1.add("SELECT").newLine();
                newSubquery1.indentEnclose(() -> {
                    newSubquery1.addItemsWithNlDel(", ", selectItems).newLine();
                });
                newSubquery1.add("FROM").newLine();
                newSubquery1.indentEnclose(() -> {
                    newSubquery1.addSuperStep(lhsResult.toSql());
                });

                newSubquery = newSubquery1;
                newTableAlias = aliasGenerator.nextAliasIfTableNameAbsent(null);
                newColumnAliases = lhsResult.getColumnAliases().entrySet()
                        .stream()
                        .collect(TieredMap.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue().rename(newTableAlias, null)));
            }

            if (hasWhereClause) {
                newSubquery.newLine();
                newSubquery.indentEnclose(() -> {
                    newSubquery.add("AND").spaces(1).addSuperStepWithIndent(notInClause);
                });
            } else {
                newSubquery.newLine().add("WHERE").newLine();
                newSubquery.indentEnclose(() -> {
                    newSubquery.addSuperStepWithIndent(notInClause);
                });
            }
            return QueryGenerateResult.of(newSubquery, newTableAlias, newColumnAliases);
        }

        private QueryGenerateResult synthesizeJoin(JoinOperator joinType, List<Op> eqConjuncts,
                                                   List<Op> otherConjuncts,
                                                   QueryGenerateResult lhsResult,
                                                   QueryGenerateResult rhsResult) {
            TieredMap<Integer, ColumnAlias> columnAlias =
                    lhsResult.getColumnAliases().merge(rhsResult.getColumnAliases());
            Function<Op, String> opToSql = OpUtil.toOpToSqlConverter(columnAlias);
            Optional<List<String>> eqConditions = OpUtil.conjunctsToSql(eqConjuncts, opToSql);
            Optional<List<String>> otherConditions = OpUtil.conjunctsToSql(otherConjuncts, opToSql);
            PrettyPrinter newSubquery = new PrettyPrinter();
            newSubquery.addSuperStepWithIndent(lhsResult.toSql()).newLine();
            newSubquery.add(joinType.toSql()).newLine();
            newSubquery.addSuperStepWithIndent(rhsResult.toSql()).newLine();
            if (eqConditions.isPresent() || otherConditions.isPresent()) {
                Iterator<String> conditions = Iterables.concat(
                        eqConditions.orElse(Collections.emptyList()),
                        otherConditions.orElse(Collections.emptyList())).iterator();
                Preconditions.checkArgument(conditions.hasNext());
                newSubquery.add("ON").spaces(1).add(conditions.next());
                newSubquery.indentEnclose(3, () -> {
                    conditions.forEachRemaining(s -> newSubquery.newLine().add("AND").spaces(1).add(s));
                });
            }

            // left anti/semi join only outputs lhs columns
            if (joinType.isLeftSemiAntiJoin()) {
                columnAlias = lhsResult.getColumnAliases();
            }
            return QueryGenerateResult.of(newSubquery, null, columnAlias);
        }

        @Override
        public QueryGenerateResult visitStarJoin(StarJoinPiece joinPiece, QueryGenerateContext context) {
            PlanPiece centre = joinPiece.getCentre();
            List<StarJoinPiece.StarCorner> corners = joinPiece.getCorners();
            int numCorners = corners.size();
            Preconditions.checkArgument(numCorners + 1 == context.getInputResults().size());
            QueryGenerateResult accQueryGenerateResult = context.getInputResults().get(0);
            boolean accHasWhereClause = !centre.getConjuncts().isEmpty();

            for (int i = 0; i < numCorners; ++i) {
                StarJoinPiece.StarCorner corner = corners.get(i);
                QueryGenerateResult cornerResult = context.getInputResults().get(i + 1);
                JoinOperator joinType = corner.getJoinType();
                List<Op> eqConjuncts = corner.getEqConjuncts();
                List<Op> otherConjuncts = corner.getOtherConjuncts();
                if (joinType.isNullAwareLeftAntiJoin()) {
                    accQueryGenerateResult = synthesizeNotInSubquery(eqConjuncts, accHasWhereClause,
                            accQueryGenerateResult, cornerResult);
                    accHasWhereClause = true;
                } else {
                    accQueryGenerateResult = synthesizeJoin(joinType, eqConjuncts, otherConjuncts,
                            accQueryGenerateResult, cornerResult);
                    accHasWhereClause = false;
                }
            }
            String tableAlias = isCentre(joinPiece) ? null : aliasGenerator.nextAliasIfTableNameAbsent(null);
            return synthesizeSubquery(joinPiece, tableAlias, accQueryGenerateResult, context);
        }

        private boolean isCentre(StarJoinPiece joinPiece) {
            if (!(joinPiece.getAuxState().getParent() instanceof StarJoinPiece)) {
                return false;
            }
            StarJoinPiece parent = joinPiece.getAuxState().getParent().cast();
            return parent.getCentre() == joinPiece;
        }

        QueryGenerateResult generate(PlanPiece piece) {
            Preconditions.checkArgument(piece.isAggregate());
            piece.assignPieceIds();
            ColumnRefSet outputColumnIds = ColumnRefSet.createByIds(piece.getColumns().keySet());
            List<Pair<Integer, GenericColumn>> outputColumns = piece.getOutputColumns(outputColumnIds);
            QueryGenerateContext context = QueryGenerateContext.of(outputColumns, outputColumnIds);
            return generateImpl(piece, context);
        }

        private void collectTraceLogs(PlanPiece planPiece, QueryGenerateContext superiorContext,
                                      QueryGenerateContext queryGenerateContext,
                                      QueryGenerateResult result) {
            if (!optTraceLog.isPresent()) {
                return;
            }
            PrettyPrinter traceLog = optTraceLog.get();
            String pieceName = planPiece.getClass().getSimpleName();
            traceLog.add("[").add(planPiece.getAuxState().getId()).add("]")
                    .spaces(1).add(pieceName).add(":").newLine();
            traceLog.indentEnclose(() -> {
                traceLog.add("Subquery:").newLine();
                traceLog.indentEnclose(() -> {
                    traceLog.addSuperStepWithIndent(result.getSubquery()).newLine();
                });
                String tableAlias = Optional.ofNullable(result.getTableAlias()).orElse("(N/A)");
                traceLog.add("TableAlias:").spaces(1).add(tableAlias).newLine();
                traceLog.add("ColumnAliases:").newLine();
                traceLog.indentEnclose(() -> {
                    result.getColumnAliases().format("ColumnAliases", traceLog);
                });
                traceLog.add("OutputColumns:").newLine();
                traceLog.indentEnclose(() -> {
                    queryGenerateContext.getOutputColumns().forEach(p -> {
                        traceLog.add(p.first).add(":")
                                .add(result.getColumnAliases().get(p.first).getQualifiedName())
                                .add(" = ").add(p.second)
                                .newLine();
                    });
                });
                traceLog.add("SuperiorInputColumnIds: ").add(superiorContext.getInputColumnIds()).newLine();
                traceLog.add("InputColumnIds: ").add(queryGenerateContext.getInputColumnIds()).newLine();
            });
        }

        private Optional<PrettyPrinter> getTraceLog() {
            return optTraceLog;
        }

        QueryGenerateResult generateImpl(PlanPiece piece, QueryGenerateContext superiorContext) {
            ColumnRefSet superiorInputColumnIds = superiorContext.getInputColumnIds();
            List<Pair<Integer, GenericColumn>> outputColumns =
                    piece.getOutputColumns(superiorInputColumnIds);
            ColumnRefSet inputColumnIds = piece.getInputColumnIds(outputColumns);
            QueryGenerateContext currentContext = QueryGenerateContext.of(outputColumns, inputColumnIds);
            List<QueryGenerateResult> results = piece.getInputPieces().stream()
                    .map(input -> generateImpl(input, currentContext))
                    .collect(Collectors.toList());
            currentContext.setInputResults(results);
            QueryGenerateResult result = piece.accept(this, currentContext);
            collectTraceLogs(piece, superiorContext, currentContext, result);
            return result;
        }
    }
}
