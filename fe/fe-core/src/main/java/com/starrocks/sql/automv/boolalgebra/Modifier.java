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

package com.starrocks.sql.automv.boolalgebra;

import com.google.common.base.Preconditions;

public enum Modifier {
    alwaysFalse(TriBool.TB_FALSE, TriBool.TB_FALSE, TriBool.TB_FALSE), //m000

    m001(TriBool.TB_FALSE, TriBool.TB_FALSE, TriBool.TB_NULL),
    m002(TriBool.TB_FALSE, TriBool.TB_FALSE, TriBool.TB_TRUE),
    m010(TriBool.TB_FALSE, TriBool.TB_NULL, TriBool.TB_FALSE),
    m011(TriBool.TB_FALSE, TriBool.TB_NULL, TriBool.TB_NULL),

    positive(TriBool.TB_FALSE, TriBool.TB_NULL, TriBool.TB_TRUE), //m012
    isNull(TriBool.TB_FALSE, TriBool.TB_TRUE, TriBool.TB_FALSE), //m020

    m021(TriBool.TB_FALSE, TriBool.TB_TRUE, TriBool.TB_NULL),

    trueOrNull(TriBool.TB_FALSE, TriBool.TB_TRUE, TriBool.TB_TRUE), //m022

    m100(TriBool.TB_NULL, TriBool.TB_FALSE, TriBool.TB_FALSE),
    m101(TriBool.TB_NULL, TriBool.TB_FALSE, TriBool.TB_NULL),
    m102(TriBool.TB_NULL, TriBool.TB_FALSE, TriBool.TB_TRUE),
    m110(TriBool.TB_NULL, TriBool.TB_NULL, TriBool.TB_FALSE),
    alwaysNull(TriBool.TB_NULL, TriBool.TB_NULL, TriBool.TB_NULL), //m111
    m112(TriBool.TB_NULL, TriBool.TB_NULL, TriBool.TB_TRUE),
    m120(TriBool.TB_NULL, TriBool.TB_TRUE, TriBool.TB_FALSE),
    m121(TriBool.TB_NULL, TriBool.TB_TRUE, TriBool.TB_NULL),
    m122(TriBool.TB_NULL, TriBool.TB_TRUE, TriBool.TB_TRUE),
    m200(TriBool.TB_TRUE, TriBool.TB_FALSE, TriBool.TB_FALSE),
    m201(TriBool.TB_TRUE, TriBool.TB_FALSE, TriBool.TB_NULL),

    isNotNull(TriBool.TB_TRUE, TriBool.TB_FALSE, TriBool.TB_TRUE), //m202
    negative(TriBool.TB_TRUE, TriBool.TB_NULL, TriBool.TB_FALSE), //m210

    m211(TriBool.TB_TRUE, TriBool.TB_NULL, TriBool.TB_NULL),
    m212(TriBool.TB_TRUE, TriBool.TB_NULL, TriBool.TB_TRUE),

    falseOrNull(TriBool.TB_TRUE, TriBool.TB_TRUE, TriBool.TB_FALSE), //m220

    m221(TriBool.TB_TRUE, TriBool.TB_TRUE, TriBool.TB_NULL),

    alwaysTrue(TriBool.TB_TRUE, TriBool.TB_TRUE, TriBool.TB_TRUE); //m222
    private static final byte[] COMPOSE_RESULTS = new byte[27 * 27];

    static {
        for (int i = 0; i < 27; ++i) {
            for (int j = 0; j < 27; ++j) {
                COMPOSE_RESULTS[i * 27 + j] = (byte) compose(Modifier.values()[i], Modifier.values()[j]).ordinal();
            }
        }
    }

    final byte bits;

    Modifier(TriBool falseBit, TriBool nullBit, TriBool trueBit) {
        this.bits = (byte) ((falseBit.ordinal() << 4) | (nullBit.ordinal() << 2) | (trueBit.ordinal()));
    }

    private static Modifier cons(TriBool falseBit, TriBool nullBit, TriBool trueBit) {
        int ordinal = falseBit.ordinal() * 9 + nullBit.ordinal() * 3 + trueBit.ordinal();
        return Modifier.values()[ordinal];
    }

    private static Modifier compose(Modifier lhs, Modifier rhs) {
        TriBool falseBit = lhs.transfer(rhs.transfer(0).ordinal());
        TriBool nullBit = lhs.transfer(rhs.transfer(1).ordinal());
        TriBool trueBit = lhs.transfer(rhs.transfer(2).ordinal());
        return cons(falseBit, nullBit, trueBit);
    }

    public TriBool transfer(int n) {
        Preconditions.checkArgument(0 <= n && n <= 2);
        return TriBool.values()[(((int) this.bits) >> ((2 - n) << 1)) & 0x3];
    }

    public Modifier compose(Modifier rhs) {
        return Modifier.values()[COMPOSE_RESULTS[this.ordinal() * 27 + rhs.ordinal()]];
    }

    public Modifier and(Modifier rhs) {
        TriBool falseBit = rhs.transfer(0).and(this.transfer(0));
        TriBool nullBit = rhs.transfer(1).and(this.transfer(1));
        TriBool trueBit = rhs.transfer(2).and(this.transfer(2));
        return cons(falseBit, nullBit, trueBit);
    }

    public Modifier or(Modifier rhs) {
        TriBool falseBit = rhs.transfer(0).or(this.transfer(0));
        TriBool nullBit = rhs.transfer(1).or(this.transfer(1));
        TriBool trueBit = rhs.transfer(2).or(this.transfer(2));
        return cons(falseBit, nullBit, trueBit);
    }
}