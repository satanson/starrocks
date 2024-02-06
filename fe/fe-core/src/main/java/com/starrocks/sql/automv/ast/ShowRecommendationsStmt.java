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

package com.starrocks.sql.automv.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.Optional;

public class ShowRecommendationsStmt extends ShowStmt {
    private static final String ID = "Id";
    private static final String MV_NAME = "Name";
    private static final String RECOMMENDED_MV = "RecommendedMV";
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column(ID, Type.INT))
            .addColumn(new Column(MV_NAME, Type.STRING))
            .addColumn(new Column(RECOMMENDED_MV, Type.STRING))
            .build();
    private TableName tableName;
    private long limit = -1;
    private long offset = -1;

    public ShowRecommendationsStmt(TableName tableName, long limit, long offset) {
        super(NodePosition.ZERO);
        this.tableName = tableName;
        this.limit = limit;
        this.offset = offset;
    }

    public TableName getTableName() {
        return tableName;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    public Optional<Long> getLimit() {
        return Optional.ofNullable(limit < 0 ? null : limit);
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public Optional<Long> getOffset() {
        return Optional.ofNullable(offset < 0 ? null : offset);
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowRecommendationsStmt(this, context);
    }
}
