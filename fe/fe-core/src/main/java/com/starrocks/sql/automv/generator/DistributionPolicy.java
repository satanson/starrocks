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

import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.Util;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DistributionPolicy {
    public static PrettyPrinter getDistribution(AggregatePiece aggPiece, Map<Integer, ColumnAlias> columnAliases) {
        PrettyPrinter printer = new PrettyPrinter();

        List<String> bucketColumns = aggPiece.getDimensions().keySet().stream()
                .map(columnAliases::get)
                .map(ColumnAlias::getName).collect(Collectors.toList());

        List<TablePiece> tablePieces = PlanPiece.collect(aggPiece, TablePiece.class);
        double harmonyDivider = tablePieces.stream().map(tablePiece ->
                1.0 / Util.downcast(tablePiece.getTable().getTable(), OlapTable.class)
                        .map(olapTable -> Math.max(1, olapTable.getDefaultDistributionInfo().getBucketNum()))
                        .orElse(1)
        ).reduce(0.0, Double::sum);
        int harmonyMean = (int) (tablePieces.size() / harmonyDivider);
        int bucketNum = Math.max(harmonyMean, 64);
        if (!bucketColumns.isEmpty()) {
            printer.add("DISTRIBUTED BY HASH").spaces(1).add("(")
                    .addItems(", ", bucketColumns).add(")")
                    .add(" BUCKETS ").add(bucketNum)
                    .newLine();
        } else {
            printer.add("DISTRIBUTED BY HASH").newLine();
        }
        return printer;
    }
}
