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

import com.starrocks.catalog.Type;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.StrictOp;

import java.util.Objects;

public class DerivedColumn extends GenericColumn {
    private final StrictOp expr;
    private transient GenericColumn norm;

    public DerivedColumn(Op expr) {
        this.expr = expr.strict();
    }

    @Override
    public Type getType() {
        return expr.getOp().getType();
    }

    public StrictOp getExpr() {
        return expr;
    }

    @Override
    public String getFQName() {
        return expr.getOp().toString();
    }

    @Override
    protected String toStringImpl() {
        return "D:" + getFQName();
    }

    @Override
    public GenericColumn clone() {
        return GenericColumn.derived(expr.getOp());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DerivedColumn that = (DerivedColumn) o;
        return Objects.equals(expr, that.expr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expr);
    }
}
