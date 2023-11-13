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

package com.starrocks.sql.automv.lattice;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.policies.AggregatePolicies;

import java.util.List;
import java.util.Objects;

public class LatticeNode {
    private final LatticeNodeId id;
    private List<LatticeNode> parent;
    private List<LatticeNode> children;
    private List<AggregatePiece> rollupAblePieces;
    private List<AggregatePiece> rollupUnablePieces;

    public LatticeNode(LatticeNodeId id, AggregatePiece aggPiece) {
        this.id = Objects.requireNonNull(id);
        this.rollupAblePieces = Lists.newArrayList();
        this.rollupUnablePieces = Lists.newArrayList();
        this.parent = Lists.newArrayList();
        this.children = Lists.newArrayList();
        Preconditions.checkArgument(aggPiece.getDistinctMetrics().isEmpty());
        boolean rollupUnable = AggregatePolicies.hasRollupUnable(aggPiece.getMetrics().values());
        if (rollupUnable) {
            rollupUnablePieces.add(aggPiece);
        } else {
            rollupAblePieces.add(aggPiece);
        }
    }

    public List<AggregatePiece> getRollupAblePieces() {
        return rollupAblePieces;
    }

    public void setRollupAblePieces(List<AggregatePiece> rollupAblePieces) {
        this.rollupAblePieces = rollupAblePieces;
    }

    public List<AggregatePiece> getRollupUnablePieces() {
        return rollupUnablePieces;
    }

    public void setRollupUnablePieces(List<AggregatePiece> rollupUnablePieces) {
        this.rollupUnablePieces = rollupUnablePieces;
    }

    public LatticeNodeId getId() {
        return id;
    }

    public List<LatticeNode> getParent() {
        return parent;
    }

    public void setParent(List<LatticeNode> parent) {
        this.parent = parent;
    }

    public List<LatticeNode> getChildren() {
        return children;
    }

    public void setChildren(List<LatticeNode> children) {
        this.children = children;
    }
}