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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Map;

public class ColumnRefToIdConverter {
    final Map<Integer, Integer> ids = Maps.newHashMap();
    int id = 0;

    public int nextId() {
        return ++id;
    }

    public int getId(ColumnRefOperator columRef) {
        return ids.computeIfAbsent(columRef.getId(), (i) -> ++id);
    }
}
