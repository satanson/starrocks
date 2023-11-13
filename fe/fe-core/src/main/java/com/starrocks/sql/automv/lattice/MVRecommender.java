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

import com.google.api.client.util.Lists;
import com.starrocks.sql.automv.generator.AggregateMVGenerator;
import com.starrocks.sql.automv.generator.ColumnRefToIdConverter;
import com.starrocks.sql.automv.generator.MVGenerateContext;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.generator.QueryGenerateResult;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.PlanPieceNormalizer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MVRecommender {
    public static List<QueryGenerateResult> recommend(List<PlanPiece> planPieces) {
        planPieces.forEach(PlanPiece::assignPieceIds);
        List<PlanPiece> normalizedPieces = planPieces.stream()
                .map(PlanPieceNormalizer::normalize)
                .collect(Collectors.toList());

        Map<String, List<PlanPiece>> pieceGroups =
                normalizedPieces.stream().collect(Collectors.groupingBy(PlanPiece::getNormHash));
        List<QueryGenerateResult> resultList = Lists.newArrayList();
        for (List<PlanPiece> pieceGroup : pieceGroups.values()) {
            if (pieceGroup.size() == 1) {
                resultList.addAll(recommendOneMv(pieceGroup.get(0)));
            } else {
                resultList.addAll(recommendMVBasedLattice(pieceGroup));
            }
        }
        return resultList;
    }

    private static List<QueryGenerateResult> recommendOneMv(PlanPiece piece) {
        AggregatePiece aggPiece = piece.cast();
        ColumnRefToIdConverter idConverter = aggPiece.getFlatTable().getCommonState().getIdConverter();
        MVGenerateContext mvGenerateContext = MVGenerateContext.builder()
                .enableGenerateTraceLog()
                .enablePolicyTraceLog()
                .setMvNameGenerator(query -> MVName.generateFromQuery(query).toString())
                .setNextId(idConverter::nextId)
                .build();
        return Collections.singletonList(AggregateMVGenerator.generate(aggPiece, mvGenerateContext));
    }

    private static List<QueryGenerateResult> recommendMVBasedLattice(List<PlanPiece> pieces) {
        AggregatePiece firstAggPiece = pieces.get(0).cast();
        Lattice lattice = Lattice.createLattice(firstAggPiece);
        pieces.forEach(piece -> lattice.insert(piece.cast()));
        lattice.consolidateRollupAblePieces();
        lattice.consolidateAll();
        return lattice.getAllPieces().stream()
                .map(MVRecommender::recommendOneMv)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }
}
