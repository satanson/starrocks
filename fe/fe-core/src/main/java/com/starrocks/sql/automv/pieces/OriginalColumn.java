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

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.sql.automv.util.PrettyPrinter;

import java.util.Objects;

public class OriginalColumn extends GenericColumn {
    private final TableName fqTableName;
    private final Column column;
    private transient String fqName = null;

    public OriginalColumn(TableName fqTableName, Column column) {
        this.fqTableName = Objects.requireNonNull(fqTableName);
        this.column = Objects.requireNonNull(column);
    }

    @Override
    public String getFQName() {
        if (fqName == null) {
            fqName = new PrettyPrinter()
                    .add(fqTableName.toSql()).add(".")
                    .addBacktickQuoted(column.getName()).getResult();
        }
        return fqName;
    }

    @Override
    public Type getType() {
        return column.getType();
    }

    @Override
    public String getColumnName() {
        return column.getName();
    }

    @Override
    protected String toStringImpl() {
        return "O:" + getFQName();
    }

    @Override
    public GenericColumn clone() {
        return GenericColumn.original(fqTableName, column);
    }

    public GenericColumn normalize(TableName uniqueFqTableName) {
        this.setNorm(GenericColumn.original(uniqueFqTableName, column));
        return this;
    }

    public GenericColumn normalize() {
        this.setNorm(this.clone());
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OriginalColumn that = (OriginalColumn) o;
        return this.getFQName().equals(that.getFQName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFQName());
    }
}
