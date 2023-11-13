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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Type;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.pieces.GenericColumn;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredMap;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PartitionPolicy {

    private static List<GenericColumn> getPartitionColumns(TablePiece tablePiece) {
        FQTable fqTable = tablePiece.getTable();
        Optional<List<String>> optPartitionColNames =
                Optional.ofNullable(fqTable.getTable().getPartitionColumnNames());

        return optPartitionColNames.map(strings -> strings.stream()
                .map(name -> fqTable.getTable().getColumn(name))
                .map(column -> GenericColumn.original(fqTable.getFqTableName(), column))
                .collect(Collectors.toList())).orElse(Collections.emptyList());
    }

    private static int typeWeight(Type type) {
        if (type.isDate()) {
            return 1;
        } else if (type.isDatetime()) {
            return 2;
        } else if (type.isIntegerType()) {
            return 3;
        } else if (type.isStringType()) {
            return 4;
        } else {
            return 5;
        }
    }

    public static PrettyPrinter getPartitionExpr(AggregatePiece aggPiece,
                                                 TieredMap<Integer, ColumnAlias> columnAliases) {
        List<TablePiece> tablePieces = PlanPiece.collect(aggPiece, TablePiece.class);
        Preconditions.checkArgument(!tablePieces.isEmpty());
        List<GenericColumn> candiPartitionColumns = tablePieces.stream()
                .map(PartitionPolicy::getPartitionColumns)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        PrettyPrinter printer = new PrettyPrinter();
        if (!candiPartitionColumns.isEmpty()) {
            Set<String> normSet = candiPartitionColumns.stream()
                    .map(GenericColumn::getNorm)
                    .map(GenericColumn::toString)
                    .collect(Collectors.toSet());

            List<Integer> candiPartitionColumnIds = aggPiece.getFlatTable().getColumns().entrySet().stream()
                    .filter(e -> normSet.contains(e.getValue().getNorm().toString()))
                    .sorted(Comparator.comparingInt(e -> typeWeight(e.getValue().getType())))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            for (Integer candiId : candiPartitionColumnIds) {
                Optional<String> optPartitionColumn =
                        Optional.ofNullable(columnAliases.get(candiId)).map(ColumnAlias::getName);
                if (optPartitionColumn.isPresent()) {
                    return printer.add("PARTITION BY ").add(optPartitionColumn.get()).newLine();
                }
            }
        }
        return printer;
    }
}
