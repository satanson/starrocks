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

package com.starrocks.sql.automv.lattice;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.GenericColumn;
import com.starrocks.sql.automv.pieces.PieceAuxState;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Lattice {
    private final Map<String, Integer> columnToOrdinalMap;
    private final List<String> columns;
    private final PlanPiece flatTable;
    private final LatticeNode root;

    private final List<LatticeNode> nodes = Lists.newArrayList();

    public Lattice(Map<String, Integer> columnToOrdinalMap, List<String> columns, PlanPiece flatTable,
                   LatticeNode root) {
        this.columnToOrdinalMap = Objects.requireNonNull(columnToOrdinalMap);
        this.columns = Objects.requireNonNull(columns);
        this.flatTable = Objects.requireNonNull(flatTable);
        this.root = Objects.requireNonNull(root);
    }

    private static AggregatePiece createRootPiece(PlanPiece piece) {
        return AggregatePiece.newBuilder()
                .setFlatTable(piece)
                .setDimensions(piece.getColumns())
                .setRollupDimensions(TieredMap.genesis())
                .setMetrics(TieredMap.genesis())
                .setDistinctMetrics(TieredMap.genesis())
                .setConjuncts(TieredList.genesis())
                .setCommonState(piece.getCommonState())
                .setAuxState(new PieceAuxState())
                .build().cast();
    }

    public static Lattice createLattice(PlanPiece seed) {
        PlanPiece flatTable = seed.cast(AggregatePiece.class).map(AggregatePiece::getFlatTable).orElse(seed);
        List<String> columns = flatTable.getColumns().values()
                .stream()
                .map(column -> column.getNorm().toString())
                .sorted()
                .distinct()
                .collect(Collectors.toList());

        Supplier<Integer> idGen = Util.nextIdGenerator();
        Map<String, Integer> columnToOrdinalMap = columns.stream()
                .collect(ImmutableMap.toImmutableMap(Function.identity(), (k) -> idGen.get()));

        AggregatePiece root = createRootPiece(flatTable);
        LatticeNodeId rootId = LatticeNodeId.calcRootId(columns.size());
        LatticeNode rootNode = new LatticeNode(rootId, root);
        return new Lattice(columnToOrdinalMap, columns, flatTable, rootNode);
    }

    private static List<AggregatePiece> consolidate(List<AggregatePiece> pieces) {
        if (pieces.size() < 2) {
            return pieces;
        }
        AggregatePiece firstAggPiece = pieces.get(0);
        List<AggregatePiece> restAggPieces = pieces.subList(1, pieces.size());
        Set<String> uniqueMetricNorms = Sets.newHashSet();
        uniqueMetricNorms.addAll(firstAggPiece.getMetrics().values().stream()
                .map(column -> column.getNorm().toString()).collect(Collectors.toList()));

        Map<String, Op> normToOpMap = firstAggPiece.getFlatTable().getColumns().entrySet()
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        e -> e.getValue().getNorm().toString(),
                        e -> OpUtil.columnToOp(e.getKey(), e.getValue())));

        TieredMap<Integer, GenericColumn> mergedMetrics = firstAggPiece.getMetrics();

        for (AggregatePiece aggPiece : restAggPieces) {
            List<GenericColumn> metrics = aggPiece.getMetrics().values().stream()
                    .filter(metric -> !uniqueMetricNorms.contains(metric.getNorm().toString()))
                    .map(metric -> Pair.create(metric, metric.getNorm().toString()))
                    .sorted(Pair.comparingBySecond())
                    .distinct()
                    .map(p -> p.first)
                    .collect(Collectors.toList());

            if (metrics.isEmpty()) {
                continue;
            }
            uniqueMetricNorms.addAll(metrics.stream().map(metric -> metric.getNorm().toString())
                    .collect(Collectors.toList()));
            ColumnRefSet usedColumns = ColumnRefSet.of();
            metrics.forEach(metric -> metric.getUsedColumns().ifPresent(usedColumns::union));
            Map<Integer, Op> idToOpMap = aggPiece.getFlatTable().getColumns()
                    .entrySet()
                    .stream()
                    .filter(e -> usedColumns.contains(e.getKey()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> Objects.requireNonNull(normToOpMap.get(e.getValue().getNorm().toString()))));
            TieredMap<Integer, GenericColumn> newMetrics =
                    OpUtil.subst(metrics, idToOpMap, firstAggPiece.getCommonState().getIdConverter());
            mergedMetrics = mergedMetrics.merge(newMetrics);
        }

        AggregatePiece newAggPiece = firstAggPiece.builder()
                .mustCast(AggregatePiece.Builder.class)
                .setMetrics(mergedMetrics)
                .build();
        return Collections.singletonList(newAggPiece);
    }

    public void insert(AggregatePiece aggPiece) {
        Preconditions.checkArgument(aggPiece.getFlatTable().getAuxState().getNormHash()
                .equals(flatTable.getAuxState().getNormHash()));
        LatticeNodeId id = LatticeNodeId.calcId(aggPiece.getDimensions().values(), columnToOrdinalMap);
        insertWithId(root, aggPiece, id);
    }

    private void insertWithId(LatticeNode node, AggregatePiece aggPiece, LatticeNodeId id) {
        TieredList<LatticeNode> commonPath = TieredList.genesis();
        List<TieredList<LatticeNode>> ancestorPathList = seekFor(node, commonPath, id);
        Preconditions.checkArgument(!ancestorPathList.isEmpty());
        List<TieredList<LatticeNode>> pathListEndWithTargetId = ancestorPathList.stream()
                .filter(ancestorPath -> ancestorPath.get(-1).getId().equals(id))
                .collect(Collectors.toList());
        if (!pathListEndWithTargetId.isEmpty()) {
            LatticeNode existingNode = pathListEndWithTargetId.get(0).get(-1);
            Preconditions.checkArgument(
                    pathListEndWithTargetId.stream().allMatch(path -> path.get(-1) == existingNode));
            existingNode.getRollupAblePieces().add(aggPiece);
        } else {
            LatticeNode newChild = new LatticeNode(id, aggPiece);
            nodes.add(newChild);
            Set<LatticeNode> parents =
                    ancestorPathList.stream().map(path -> path.get(-1)).collect(Collectors.toSet());
            // step1: Set newChild's parents.
            newChild.setParent(Lists.newArrayList(parents));

            Set<LatticeNode> grandChildren = Sets.newHashSet();

            // step2: Set parents' children.
            // Some children of parents are newChild's siblings, the others are demoted to be grandchildren
            // of the parents, in another word, they are children of the newChild, so we gather them together.
            for (LatticeNode parent : parents) {
                Map<Boolean, List<LatticeNode>> childGroups = parent.getChildren().stream()
                        .collect(Collectors.partitioningBy(child -> child.getId().isCoveredStrictlyBy(id)));
                grandChildren.addAll(childGroups.get(true));
                List<LatticeNode> siblings = childGroups.get(false);
                siblings.add(newChild);
                parent.setChildren(siblings);
            }
            // deduplicate grandchildren
            // step3: Set newChild's children to be grandChildren of parents.
            newChild.getChildren().addAll(grandChildren);

            // step4: Set grandChildren's parents.
            // grandChildren's current parents should be preserved if it does not cover
            // newChild. the newChild should be added to grandchildren's parents.
            for (LatticeNode grandChild : grandChildren) {
                List<LatticeNode> siblings = grandChild.getParent().stream()
                        .filter(parent -> !parent.getId().isCovering(id))
                        .collect(Collectors.toList());
                siblings.add(newChild);
                grandChild.setParent(siblings);
            }
        }
    }

    private List<TieredList<LatticeNode>> seekFor(LatticeNode node, TieredList<LatticeNode> commonPath,
                                                  LatticeNodeId id) {
        Preconditions.checkArgument(node.getId().isCovering(id));
        TieredList<LatticeNode> newCommonPath = commonPath.concatOne(node);
        List<LatticeNode> superSetNodes = node.getChildren().stream()
                .filter(child -> child.getId().isCovering(id))
                .collect(Collectors.toList());

        if (superSetNodes.isEmpty()) {
            return Collections.singletonList(newCommonPath);
        }

        return superSetNodes.stream().map(n -> seekFor(n, newCommonPath, id))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public void consolidateRollupAblePieces() {
        nodes.forEach(node -> {
            node.setRollupAblePieces(consolidate(node.getRollupAblePieces()));
        });
    }

    public void consolidateAll() {
        nodes.forEach(node -> {
            if (!node.getRollupUnablePieces().isEmpty()) {
                List<AggregatePiece> pieces = Lists.newArrayListWithCapacity(
                        node.getRollupAblePieces().size() + node.getRollupUnablePieces().size());
                pieces.addAll(node.getRollupAblePieces());
                pieces.addAll(node.getRollupUnablePieces());
                node.setRollupAblePieces(Lists.newArrayList());
                node.setRollupUnablePieces(consolidate(pieces));
            }
        });
    }

    List<AggregatePiece> getAllPieces() {
        return nodes.stream().flatMap(node -> Stream.of(node.getRollupAblePieces(), node.getRollupUnablePieces()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }
}
