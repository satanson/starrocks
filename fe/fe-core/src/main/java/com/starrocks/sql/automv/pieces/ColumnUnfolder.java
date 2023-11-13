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

import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.util.TieredMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ColumnUnfolder {
    private TieredMap<Integer, GenericColumn> columns;

    public ColumnUnfolder(TieredMap<Integer, GenericColumn> columns) {
        this.columns = Objects.requireNonNull(columns);
    }

    public List<String> unfold(TieredMap<Integer, GenericColumn> newColumns) {
        Map<Boolean, TieredMap<Integer, GenericColumn>> columnGroups = newColumns.entrySet().stream()
                .collect(Collectors.partitioningBy(e -> e.getValue().isOriginal(), TieredMap.toMap()));
        TieredMap<Integer, GenericColumn> unfoldNewColumns = TieredMap.genesis();
        // process OriginalColumns at first, then DerivedColumn, since the latter may be reference the former
        for (TieredMap<Integer, GenericColumn> group : Arrays.asList(columnGroups.get(true), columnGroups.get(false))) {
            TieredMap<Integer, GenericColumn> newGroup = group.entrySet()
                    .stream()
                    .collect(TieredMap.toMap(Map.Entry::getKey, e -> e.getValue().unfold(columns)));

            unfoldNewColumns = unfoldNewColumns.merge(newGroup);
            columns = columns.merge(newGroup);
        }
        return unfoldNewColumns.values().stream()
                .map(GenericColumn::getNorm)
                .map(GenericColumn::toString)
                .sorted()
                .collect(Collectors.toList());
    }

    public List<String> unfold(List<Op> conjuncts) {
        return conjuncts.stream()
                .map(op -> OpUtil.unfoldOp(op, columns))
                .map(Op::getNorm)
                .map(Op::toString)
                .sorted()
                .collect(Collectors.toList());
    }
}
