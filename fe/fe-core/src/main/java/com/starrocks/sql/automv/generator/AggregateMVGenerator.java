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

import com.starrocks.common.Pair;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.GenericColumn;
import com.starrocks.sql.automv.policies.AggregatePolicies;
import com.starrocks.sql.automv.policies.AggregatePolicy;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredMap;

import java.util.List;
import java.util.stream.Collectors;

public class AggregateMVGenerator {
    public static QueryGenerateResult generate(AggregatePiece aggPiece, MVGenerateContext context) {
        PrettyPrinter mvSchema = new PrettyPrinter();
        AggregatePolicy defaultPolicy =
                AggregatePolicies.defaultPolicies(context.getNextId(), context.getPolicyTraceLog().orElse(null));
        aggPiece = defaultPolicy.convert(aggPiece).orElse(aggPiece);
        QueryGenerateResult result = QueryGenerator.generate(aggPiece, context.getGenerateTraceLog().orElse(null));
        TieredMap<Integer, ColumnAlias> columnAliases = result.getColumnAliases();

        List<String> mvColumns = result.getOrderedColumns().stream()
                .map(p -> columnAliases.get(p.first))
                .map(ColumnAlias::getName).collect(Collectors.toList());

        String mvName = context.getMvNameGenerator().apply(result.getSubquery().getResult());
        mvSchema.add("CREATE MATERIALIZED VIEW").spaces(1).add(mvName).spaces(1).add("(").newLine();
        mvSchema.indentEnclose(() -> mvSchema.addItemsWithNlDel(", ", mvColumns));
        mvSchema.newLine().add(")").newLine();
        mvSchema.addSuperStep(PartitionPolicy.getPartitionExpr(aggPiece, columnAliases));
        mvSchema.addSuperStep(DistributionPolicy.getDistribution(aggPiece, columnAliases));
        List<Pair<Integer, GenericColumn>> candidateOrderByColumns = result.getOrderedDimensions();

        for (int i = 0; i < candidateOrderByColumns.size(); ++i) {
            if (!candidateOrderByColumns.get(i).second.getType().canDistributedBy()) {
                candidateOrderByColumns = candidateOrderByColumns.subList(0, Math.min(i, 3));
                break;
            }
        }

        if (!candidateOrderByColumns.isEmpty()) {
            List<String> orderByItems = candidateOrderByColumns.stream()
                    .map(p -> columnAliases.get(p.first))
                    .map(ColumnAlias::getName).collect(Collectors.toList());
            mvSchema.add("ORDER BY (").addItems(", ", orderByItems).add(")").newLine();
        }
        mvSchema.add("REFRESH ASYNC START(\"2023-12-01 10:00:00\") EVERY(INTERVAL 1 DAY)").newLine();
        mvSchema.addSuperStep(PropertiesPolicy.getProperties(aggPiece, columnAliases));
        mvSchema.add("AS").newLine();
        mvSchema.addSuperStep(result.getSubquery());
        return result.updateSubquery(mvSchema)
                .setMvName(mvName)
                .setTraceLog(result.getTraceLog().orElse(null));
    }
}
