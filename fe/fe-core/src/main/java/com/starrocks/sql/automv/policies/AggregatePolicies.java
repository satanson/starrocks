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

package com.starrocks.sql.automv.policies;

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.GenericColumn;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.util.PrettyPrinter;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class AggregatePolicies {
    public static final ImmutableSet<String> ROLLUP_UNABLE_AGGREGATIONS = ImmutableSet.<String>builder()
            .add(FunctionSet.ANY_VALUE)
            .add(FunctionSet.CORR)
            .add(FunctionSet.COVAR_POP)
            .add(FunctionSet.COVAR_SAMP)
            .add(FunctionSet.DICT_MERGE)
            .add(FunctionSet.EXCHANGE_BYTES)
            .add(FunctionSet.EXCHANGE_SPEED)
            .add(FunctionSet.GROUP_CONCAT)
            .add(FunctionSet.HISTOGRAM)
            .add(FunctionSet.INTERSECT_COUNT)
            .add(FunctionSet.MAX_BY)
            .add(FunctionSet.MIN_BY)
            .add(FunctionSet.PERCENTILE_CONT)
            .add(FunctionSet.PERCENTILE_DISC)
            .add(FunctionSet.RETENTION)
            .add(FunctionSet.STD)
            .add(FunctionSet.STDDEV)
            .add(FunctionSet.STDDEV_POP)
            .add(FunctionSet.STDDEV_SAMP)
            .add(FunctionSet.VARIANCE)
            .add(FunctionSet.VARIANCE_POP)
            .add(FunctionSet.VARIANCE_SAMP)
            .add(FunctionSet.VAR_POP)
            .add(FunctionSet.VAR_SAMP)
            .add(FunctionSet.MULTI_DISTINCT_COUNT)
            .add(FunctionSet.MULTI_DISTINCT_SUM)
            .add(FunctionSet.WINDOW_FUNNEL)
            .build();

    public static final ImmutableSet<String> ROLLUP_CONVERTIBLE_AGGREGATIONS = ImmutableSet.<String>builder()
            .add(FunctionSet.APPROX_COUNT_DISTINCT)
            .add(FunctionSet.NDV)
            .add(FunctionSet.AVG)
            .add(FunctionSet.BITMAP_UNION_COUNT)
            .add(FunctionSet.BITMAP_UNION_INT)
            .add(FunctionSet.HLL_UNION_AGG)
            .add(FunctionSet.MULTI_DISTINCT_COUNT)
            .add(FunctionSet.MULTI_DISTINCT_SUM)
            .add(FunctionSet.PERCENTILE_APPROX)
            .add(FunctionSet.SUM)
            .add(FunctionSet.COUNT)
            .build();
    public static final ImmutableSet<String> ROLLUP_ABLE_AGGREGATIONS = ImmutableSet.<String>builder()
            .add(FunctionSet.ARRAY_AGG)
            .add(FunctionSet.ARRAY_AGG_DISTINCT)
            .add(FunctionSet.BITMAP_AGG)
            .add(FunctionSet.BITMAP_INTERSECT)
            .add(FunctionSet.BITMAP_UNION)
            .add(FunctionSet.COUNT)
            .add(FunctionSet.COUNT_IF)
            .add(FunctionSet.HLL_RAW)
            .add(FunctionSet.HLL_RAW_AGG)
            .add(FunctionSet.HLL_UNION)
            .add(FunctionSet.MAX)
            .add(FunctionSet.MIN)
            .add(FunctionSet.PERCENTILE_UNION)
            .add(FunctionSet.SUM)
            .build();

    public static boolean isRollupAble(GenericColumn metric) {
        return ROLLUP_ABLE_AGGREGATIONS.contains(OpUtil.mustGetFnName(metric));
    }

    public static boolean isRollupConvertible(GenericColumn metric) {
        return ROLLUP_CONVERTIBLE_AGGREGATIONS.contains(OpUtil.mustGetFnName(metric));
    }

    public static boolean isRollupUnable(GenericColumn metric) {
        return ROLLUP_UNABLE_AGGREGATIONS.contains(OpUtil.mustGetFnName(metric)) ||
                (!isRollupAble(metric) && !isRollupConvertible(metric));
    }

    public static boolean hasRollupUnable(Collection<GenericColumn> metrics) {
        return metrics.stream().anyMatch(
                metric -> AggregatePolicies.isRollupUnable(metric) || metric.getOp().isDistinct());
    }

    public static AggregatePolicy.AbstractAggregatePolicy distinctRollupPolicy(Supplier<Integer> idGen) {
        return AggregatePolicy.and(
                ConditionalPolicy.EXISTS_DISTINCT_METRICS,
                AggregatePolicy.seq(
                        MergeDistinctMetricsIntoMetricsPolicy.getInstance(),
                        AggregatePolicy.and(
                                ConditionalPolicy.EXISTS_DISTINCT_AVG_METRICS,
                                AvgPolicy.of(idGen)),
                        BitmapBasedCountDistinctPolicy.of(idGen),
                        ArrayAggBasedCountDistinctPolicy.of(idGen),
                        HllBasedCountDistinctPolicy.of(idGen),
                        SplitDistinctMetricsFromMetricsPolicy.getInstance()));
    }

    public static AggregatePolicy.AbstractAggregatePolicy defaultPolicies(Supplier<Integer> idGen,
                                                                          PrettyPrinter traceLog) {
        AggregatePolicy.AbstractAggregatePolicy basicPolicies = AggregatePolicy.seq(
                AvgPolicy.of(idGen),
                distinctRollupPolicy(idGen),
                BitmapPolicy.of(idGen),
                HllPolicy.of(idGen),
                PercentilePolicy.of(idGen));

        AggregatePolicy.AbstractAggregatePolicy policy =
                AggregatePolicy.seq(
                        EliminateSemiAntiJoinPolicy.getInstance(),
                        AggregatePolicy.or(
                                RollupAblePolicy.getInstance(),
                                AggregatePolicy.and(
                                        ConditionalPolicy.EXISTS_ROLLUP_REWRITABLE_BUT_ROLLUP_UNABLE_METRICS,
                                        basicPolicies,
                                        RollupAblePolicy.getInstance()),
                                RollupUnablePolicy.getInstance()));

        return Optional.ofNullable(traceLog).map(log -> AggregatePolicy.trace(policy, traceLog, 1)).orElse(policy);
    }

    public static AggregatePolicy defaultPolicies(Supplier<Integer> idGen) {
        return defaultPolicies(idGen, null);
    }

    public static PlanPiece perfectMatch(PlanPiece piece) {
        Optional<AggregatePiece> optAggPiece = piece.cast(AggregatePiece.class).
                flatMap(RollupUnablePolicy.getInstance()::convert);
        if (optAggPiece.isPresent()) {
            return optAggPiece.get();
        } else {
            return piece;
        }
    }

    public static final class RollupUnablePolicy extends AggregatePolicy.SimplePolicy {

        private static final RollupUnablePolicy INSTANCE = new RollupUnablePolicy();

        private RollupUnablePolicy() {
        }

        public static RollupUnablePolicy getInstance() {
            return INSTANCE;
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return Optional.of(aggPiece.toPerfect());
        }
    }

    public static final class RollupAblePolicy extends AggregatePolicy.SimplePolicy {
        private static final RollupAblePolicy INSTANCE = new RollupAblePolicy();

        private RollupAblePolicy() {
        }

        public static RollupAblePolicy getInstance() {
            return INSTANCE;
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            boolean allRollupAble = aggPiece.getMetrics()
                    .values().stream()
                    .allMatch(AggregatePolicies::isRollupAble);
            if (allRollupAble && aggPiece.getDistinctMetrics().isEmpty()) {
                return Optional.of(aggPiece.toRollup());
            } else {
                return Optional.empty();
            }
        }
    }

    public static final class BitmapPolicy extends AggregatePolicy.SimplePolicy {
        private final Supplier<Integer> idGen;

        private BitmapPolicy(Supplier<Integer> idGen) {
            this.idGen = Objects.requireNonNull(idGen);
        }

        public static BitmapPolicy of(Supplier<Integer> idGen) {
            return new BitmapPolicy(idGen);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRollupRewriter.BITMAP_ROLLUP_REWRITER.rewrite(aggPiece, idGen);
        }
    }

    public static final class PercentilePolicy extends AggregatePolicy.SimplePolicy {
        private final Supplier<Integer> idGen;

        private PercentilePolicy(Supplier<Integer> idGen) {
            this.idGen = Objects.requireNonNull(idGen);
        }

        public static PercentilePolicy of(Supplier<Integer> idGen) {
            return new PercentilePolicy(idGen);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRollupRewriter.PERCENTILE_ROLLUP_REWRITER.rewrite(aggPiece, idGen);
        }
    }

    public static final class AvgPolicy extends AggregatePolicy.SimplePolicy {
        private final Supplier<Integer> idGen;

        private AvgPolicy(Supplier<Integer> idGen) {
            this.idGen = Objects.requireNonNull(idGen);
        }

        public static AvgPolicy of(Supplier<Integer> idGen) {
            return new AvgPolicy(idGen);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRollupRewriter.AVG_ROLLUP_REWRITER.rewrite(aggPiece, idGen);
        }
    }

    public static final class HllPolicy extends AggregatePolicy.SimplePolicy {
        private final Supplier<Integer> idGen;

        private HllPolicy(Supplier<Integer> idGen) {
            this.idGen = Objects.requireNonNull(idGen);
        }

        public static HllPolicy of(Supplier<Integer> idGen) {
            return new HllPolicy(idGen);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRollupRewriter.HLL_ROLLUP_REWRITER.rewrite(aggPiece, idGen);
        }
    }

    public static final class ArrayAggBasedCountDistinctPolicy extends AggregatePolicy.SimplePolicy {
        private final Supplier<Integer> idGen;

        public ArrayAggBasedCountDistinctPolicy(Supplier<Integer> idGen) {
            this.idGen = Objects.requireNonNull(idGen);
        }

        public static ArrayAggBasedCountDistinctPolicy of(Supplier<Integer> idGen) {
            return new ArrayAggBasedCountDistinctPolicy(idGen);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRollupRewriter.ARRAY_AGG_BASED_DISTINCT_ROLLUP_REWRITER.rewrite(aggPiece, idGen);
        }
    }

    public static final class HllBasedCountDistinctPolicy extends AggregatePolicy.SimplePolicy {
        private final Supplier<Integer> idGen;

        private HllBasedCountDistinctPolicy(Supplier<Integer> idGen) {
            this.idGen = Objects.requireNonNull(idGen);
        }

        public static HllBasedCountDistinctPolicy of(Supplier<Integer> idGen) {
            return new HllBasedCountDistinctPolicy(idGen);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRollupRewriter.HLL_AGG_BASED_DISTINCT_ROLLUP_REWRITER.rewrite(aggPiece, idGen);
        }
    }

    public static final class BitmapBasedCountDistinctPolicy extends AggregatePolicy.SimplePolicy {
        private final Supplier<Integer> idGen;

        private BitmapBasedCountDistinctPolicy(Supplier<Integer> idGen) {
            this.idGen = Objects.requireNonNull(idGen);
        }

        public static BitmapBasedCountDistinctPolicy of(Supplier<Integer> idGen) {
            return new BitmapBasedCountDistinctPolicy(idGen);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRollupRewriter.BITMAP_AGG_BASED_DISTINCT_ROLLUP_REWRITER.rewrite(aggPiece, idGen);
        }
    }

    private static final class MergeDistinctMetricsIntoMetricsPolicy extends AggregatePolicy.SimplePolicy {
        private static final MergeDistinctMetricsIntoMetricsPolicy INSTANCE =
                new MergeDistinctMetricsIntoMetricsPolicy();

        private MergeDistinctMetricsIntoMetricsPolicy() {
        }

        public static MergeDistinctMetricsIntoMetricsPolicy getInstance() {
            return INSTANCE;
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return Optional.of(aggPiece.mergeDistinctMetricsIntoMetrics());
        }
    }

    private static final class SplitDistinctMetricsFromMetricsPolicy extends AggregatePolicy.SimplePolicy {
        private static final SplitDistinctMetricsFromMetricsPolicy INSTANCE =
                new SplitDistinctMetricsFromMetricsPolicy();

        private SplitDistinctMetricsFromMetricsPolicy() {
        }

        public static SplitDistinctMetricsFromMetricsPolicy getInstance() {
            return INSTANCE;
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return Optional.of(aggPiece.splitDistinctMetricsFromMetrics());
        }
    }

    public static final class ConditionalPolicy extends AggregatePolicy.SimplePolicy {
        public static final ConditionalPolicy EXISTS_DISTINCT_METRICS =
                ConditionalPolicy.of("Exist DistinctMetrics",
                        aggPiece -> !aggPiece.getDistinctMetrics().isEmpty());
        public static final ConditionalPolicy EXISTS_DISTINCT_AVG_METRICS =
                ConditionalPolicy.of("Exist DistinctMetrics",
                        aggPiece -> aggPiece.getMetrics().values().stream().noneMatch(OpUtil::isAvgDistinct));
        public static final ConditionalPolicy EXISTS_ROLLUP_REWRITABLE_BUT_ROLLUP_UNABLE_METRICS =
                ConditionalPolicy.of("Exist rollup-rewritable but rollup-unable metrics",
                        aggPiece -> {
                            Collection<GenericColumn> columns =
                                    aggPiece.getMetrics().merge(aggPiece.getDistinctMetrics()).values();
                            boolean existsRollupRewritable = columns.stream()
                                    .anyMatch(AggregatePolicies::isRollupConvertible);
                            boolean existsNoRollupUnable = columns.stream()
                                    .noneMatch(AggregatePolicies::isRollupUnable);
                            return existsRollupRewritable && existsNoRollupUnable;
                        });
        private final String description;
        private final Predicate<AggregatePiece> predicate;

        private ConditionalPolicy(String description, Predicate<AggregatePiece> predicate) {
            this.description = Objects.requireNonNull(description);
            this.predicate = Objects.requireNonNull(predicate);
        }

        public static ConditionalPolicy of(String description, Predicate<AggregatePiece> predicate) {
            return new ConditionalPolicy(description, predicate);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            if (predicate.test(aggPiece)) {
                return Optional.of(aggPiece);
            } else {
                return Optional.empty();
            }
        }

        @Override
        public PrettyPrinter toPrettyString() {
            PrettyPrinter printer = new PrettyPrinter();
            printer.add(this.getClass().getSimpleName()).add('[').add(description).add(']');
            return printer;
        }
    }
}