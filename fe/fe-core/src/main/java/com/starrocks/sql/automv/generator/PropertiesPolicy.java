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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Table;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.system.ComputeNode;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PropertiesPolicy {
    public static int calcReplicationNum() throws NoAliveBackendException {
        if (RunMode.isSharedDataMode()) {
            return 1;
        }
        int numNodes = GlobalStateMgr.getCurrentSystemInfo().getAvailableBackends().stream()
                .map(ComputeNode::getIP).collect(Collectors.toSet()).size();
        int defaultReplicationNum = Math.min(3, numNodes);
        if (defaultReplicationNum == 0) {
            throw new NoAliveBackendException("No alive backend");
        }
        return defaultReplicationNum;
    }

    public static PrettyPrinter getProperties(AggregatePiece aggPiece, Map<Integer, ColumnAlias> columAliases) {
        List<TablePiece> tablePieces = PlanPiece.collect(aggPiece, TablePiece.class);
        List<Table> cloudTables = tablePieces.stream()
                .map(tablePiece -> tablePiece.getTable().getTable())
                .filter(Table::isCloudNativeTableOrMaterializedView)
                .collect(Collectors.toList());

        boolean hasExternalTables = tablePieces.stream()
                .map(tablePiece -> tablePiece.getTable().getTable())
                .anyMatch(table -> !table.isCloudNativeTableOrMaterializedView());

        Map<String, String> propItems = Maps.newHashMap();
        try {
            propItems.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "" + calcReplicationNum());
        } catch (NoAliveBackendException e) {
            throw new RuntimeException(e);
        }
        if (!cloudTables.isEmpty()) {
            Map<String, String> properties = cloudTables.get(0).getProperties();
            propItems.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE,
                    properties.getOrDefault(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true"));
            propItems.put(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME,
                    Objects.requireNonNull(properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME)));
        } else {
            propItems.put(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE, "true");
            propItems.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "HDD");
        }

        if (hasExternalTables) {
            propItems.put(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE, "CHECKED");
        }

        List<PrettyPrinter> items = propItems.entrySet().stream()
                .map(e -> new PrettyPrinter().addDoubleQuoted(e.getKey()).add(" = ").addDoubleQuoted(e.getValue()))
                .collect(Collectors.toList());
        PrettyPrinter printer = new PrettyPrinter();
        printer.add("PROPERTIES (").newLine();
        printer.indentEnclose(() -> printer.addSuperStepsWithNl(",", items));
        printer.newLine().add(")").newLine();
        return printer;
    }
}
