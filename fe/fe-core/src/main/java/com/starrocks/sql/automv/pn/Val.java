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

package com.starrocks.sql.automv.pn;

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.Objects;

public final class Val extends Op {
    public static final Val FALSE_VAL = Op.val(ConstantOperator.FALSE);
    public static final Val NULL_VAL = Op.val(ConstantOperator.NULL);
    public static final Val TRUE_VAL = Op.val(ConstantOperator.TRUE);
    private final ConstantOperator value;

    public Val(ConstantOperator value) {
        super(value.getType());
        this.value = value;
    }

    @Override
    public Op clone() {
        return new Val(this.value);
    }

    @Override
    protected int hashCodeImpl() {
        return Objects.hash(type, value);
    }

    @Override
    protected String toStringImpl() {
        return String.format("(val[%s] %s)", type.toSql(), value.toString());
    }

    public ConstantOperator getValue() {
        return value;
    }

    @Override
    protected SymTab getSymTabImpl() {
        return SymTab.EMPTY;
    }

    @Override
    public <R, C> R accept(OpVisitor<R, C> visitor, C context) {
        return visitor.visitVal(this, context);
    }

    @Override
    public int nary() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Val val = (Val) o;
        return Objects.equals(value, val.value);
    }

    @Override
    protected int getHeightImpl() {
        return 1;
    }
}