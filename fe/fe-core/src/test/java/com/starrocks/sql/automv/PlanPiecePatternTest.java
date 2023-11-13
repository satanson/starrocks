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

package com.starrocks.sql.automv;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.automv.ast.ShowRecommendationsStmt;
import com.starrocks.sql.automv.generator.AggregateMVGenerator;
import com.starrocks.sql.automv.generator.ColumnRefToIdConverter;
import com.starrocks.sql.automv.generator.MVGenerateContext;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.generator.QueryGenerateResult;
import com.starrocks.sql.automv.lattice.MVRecommender;
import com.starrocks.sql.automv.pattern.PlanPiecePattern;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.PlanPieceBuilder;
import com.starrocks.sql.automv.pieces.PlanPieceNormalizer;
import com.starrocks.sql.automv.pieces.PlanPiecePrinter;
import com.starrocks.sql.automv.policies.AggregatePolicies;
import com.starrocks.sql.automv.policies.AggregatePolicy;
import com.starrocks.sql.automv.qe.CustomizedQueryExecutor;
import com.starrocks.sql.automv.qe.RboOptimizer;
import com.starrocks.sql.automv.qe.TunespaceExecutor;
import com.starrocks.sql.automv.tunespace.ColumnPlus;
import com.starrocks.sql.automv.tunespace.MaterializedViewPlus;
import com.starrocks.sql.automv.tunespace.PieceTraits;
import com.starrocks.sql.automv.tunespace.PlanPieceInfo;
import com.starrocks.sql.automv.tunespace.QueryStatementPlus;
import com.starrocks.sql.automv.tunespace.TablePlus;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.optimizer.LogicalPlanPrinter;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.parser.StarRocksParser;
import com.starrocks.sql.plan.TPCDSTestUtil;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TRowFormat;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.kerby.util.IOUtil;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;
import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.connectContext;

public class PlanPiecePatternTest {
    protected static ConnectContext ctx;
    protected static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnablePipelineEngine(true);
        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME)
                .useDatabase(StatsConstants.STATISTICS_DB_NAME)
                .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        starRocksAssert.withDatabase("tpcds_db0").useDatabase("tpcds_db0");
        TPCDSTestUtil.prepareTables(starRocksAssert);
    }

    @Test
    public void testParse() {
        String sql = "select 1 from t0";
        StarRocksParser parser = SqlParser.parserBuilder(sql, ctx.getSessionVariable());
    }

    @Test
    public void testMVName() {
        MVName mvName = MVName.generateFromQuery("select l_orderkey from lineitem");
        MVName mvName2 = MVName.generateFromQuery("select l_partkey from lineitem");
        System.out.println(mvName);
        Optional<MVName> optNewMVName = MVName.parse(mvName.toString());
        Preconditions.checkArgument(optNewMVName.isPresent());
        MVName newMvName = optNewMVName.get();
        Assert.assertEquals(mvName, newMvName);
        Assert.assertTrue(mvName.collidesWith(newMvName));
        Assert.assertTrue(mvName.collidesWith(newMvName.toString()));
        Assert.assertNotEquals(mvName, mvName2);
        Assert.assertFalse(mvName.collidesWith(mvName2));
    }

    private Map<String, String> getMaterializedViews(String sql) {
        QueryStatementPlus stmt = RboOptimizer.getQueryStatement(ctx, sql);
        QueryStatement queryStmt = stmt.getQueryStatement();
        Map<String, FQTable> fqTableMap = stmt.getFqTableMap();
        Map<String, String> mvMap = Maps.newHashMap();
        List<OptExpression> subPlans = RboOptimizer.getSubPlans(queryStmt, ctx, PlanPiecePattern.getSPJG());
        for (OptExpression subPlan : subPlans) {
            ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
            Optional<AggregatePiece> optPlanPiece =
                    PlanPieceBuilder.createPlanPiece(subPlan, idConverter, fqTableMap).cast(AggregatePiece.class);
            Preconditions.checkArgument(optPlanPiece.isPresent());
            AggregatePiece planPiece = optPlanPiece.get();

            System.out.println("=================PlanPiece Norm=====================");
            PlanPieceNormalizer.normalize(planPiece);
            System.out.println(planPiece.cast(AggregatePiece.class).map(AggregatePiece::getFlatTable).orElse(planPiece)
                    .getAuxState().getNorm().getResult());

            System.out.println("=================PlanPiece=====================");
            System.out.println(PlanPiecePrinter.print(planPiece));
            System.out.println("=================PlanPiece=====================");
            MVGenerateContext mvGenerateContext = MVGenerateContext.builder()
                    .enableGenerateTraceLog()
                    .enablePolicyTraceLog()
                    .setMvNameGenerator(query -> MVName.generateFromQuery(query).toString())
                    .setNextId(idConverter::nextId)
                    .build();
            QueryGenerateResult result = AggregateMVGenerator.generate(planPiece, mvGenerateContext);
            System.out.println(mvGenerateContext.getPolicyTraceLog().get().getResult());
            mvMap.put(result.getMvName(), result.getSubquery().getResult());
        }
        return mvMap;
    }

    Function<OptExpression, PlanPiece> subPlanToPiece(Map<String, FQTable> fqTableMap) {
        return subPlan -> {
            ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
            Optional<AggregatePiece> optPlanPiece =
                    PlanPieceBuilder.createPlanPiece(subPlan, idConverter, fqTableMap).cast(AggregatePiece.class);
            Preconditions.checkArgument(optPlanPiece.isPresent());
            AggregatePolicy policy = AggregatePolicies.defaultPolicies(idConverter::nextId);
            return policy.convert(optPlanPiece.get()).get();
        };
    }

    private List<PlanPiece> getPlanPieces(String sql) {
        QueryStatementPlus stmt = RboOptimizer.getQueryStatement(ctx, sql);
        QueryStatement queryStmt = stmt.getQueryStatement();
        Map<String, FQTable> fqTableMap = stmt.getFqTableMap();
        Function<OptExpression, PlanPiece> subPlanToPieceConverter = subPlanToPiece(fqTableMap);
        return RboOptimizer.getSubPlans(queryStmt, ctx, PlanPiecePattern.getSPJG())
                .stream()
                .map(subPlanToPieceConverter)
                .collect(Collectors.toList());
    }

    @Test
    public void testMergeMV() {
        List<Pair<String, PlanPiece>> pieces = TPCDSTestUtil.getAllQueries().stream()
                .flatMap(p -> getPlanPieces(p.second).stream().map(piece -> Pair.create(p.first, piece)))
                .collect(Collectors.toList());
        pieces.forEach(p -> p.second.assignPieceIds());
        System.out.println("Piece Num=" + pieces.size());
        Map<String, List<Pair<String, PlanPiece>>> pieceGroups =
                pieces.stream().collect(Collectors.groupingBy(p -> {
                    PlanPieceNormalizer.normalize(p.second);
                    return p.second.getAuxState().getNormHash();
                }));
        System.out.println("Piece Group Num=" + pieceGroups.size());
        pieceGroups.forEach((k, v) -> {
            Set<String> querySet = v.stream().map(p -> p.first).collect(Collectors.toSet());
            String querySetStr = String.join(",", querySet);
            System.out.println("Group#" + k + ": " + v.size() + ", queries=" + querySetStr);
        });
    }

    void mockUpCustomizedQueryExecutor(java.util.function.Predicate<String> queryFilter) {
        new MockUp<CustomizedQueryExecutor>() {
            @Mock
            public <T> List<T> query(Class<T> klass, List<ColumnPlus> columns, ConnectContext context, String sql) {
                Preconditions.checkArgument(PlanPieceInfo.class.equals(klass));
                List<T> infos = (List<T>) getPlanPlanInfosOfTPCDS(queryFilter);
                return infos;
            }
        };
    }

    private List<PlanPieceInfo> getPlanPlanInfosOfTPCDS(java.util.function.Predicate<String> queryFilter) {
        return TPCDSTestUtil.getAllQueries().stream()
                .filter(p -> queryFilter.test(p.first))
                .map(p -> p.second)
                .flatMap(query -> RboOptimizer.getPlanPieces(query, ctx).stream())
                .map(planPiece -> {
                    AggregatePolicy policy =
                            AggregatePolicies.defaultPolicies(planPiece.getCommonState().getIdConverter()::nextId);
                    return PlanPieceInfo.from(planPiece, policy, planPiece.getCommonState().getFqTableMap());
                }).collect(Collectors.toList());
    }

    @Test
    public void testConsolidateMV() {
        List<Pair<String, PlanPiece>> pieces = TPCDSTestUtil.getAllQueries().stream()
                .flatMap(p -> getPlanPieces(p.second).stream().map(piece -> Pair.create(p.first, piece)))
                .collect(Collectors.toList());

        List<PlanPiece> pieceList = pieces.stream().map(p -> p.second).collect(Collectors.toList());
        System.out.println("Piece Num=" + pieces.size());
        List<QueryGenerateResult> resultList = MVRecommender.recommend(pieceList);
        resultList.forEach(mvResult -> {
            System.out.println("MVName=" + mvResult.getMvName());
            System.out.println(mvResult.getSubquery().getResult());
        });
    }

    @Test
    public void testShowRecommendations() {
        //mockUpCustomizedQueryExecutor(name -> name.equals("query23"));
        mockUpCustomizedQueryExecutor(name -> true);
        TableName tableName = new TableName(null, "db", "_tunespace_");
        ShowRecommendationsStmt stmt = new ShowRecommendationsStmt(tableName, -1, -1);
        ShowResultSet showResultSet = TunespaceExecutor.execute(stmt, ctx);
        for (List<String> row : showResultSet.getResultRows()) {
            PrettyPrinter printer = new PrettyPrinter();
            printer.addItemsWithDelNl(";", row);
            System.out.println(printer.getResult());
        }
    }

    @Test
    public void testPopolateLegacyMV() throws Exception {
        String sql = TPCDSTestUtil.getQuery("query01");
        Map<String, String> mvMap = getMaterializedViews(sql);
        for (Map.Entry<String, String> entry : mvMap.entrySet()) {
            String mvName = entry.getKey();
            String mvSchema = entry.getValue();
            starRocksAssert.withMaterializedView(mvSchema);
            String db = ctx.getDatabase();
            MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(db)
                    .getTable(mvName);
            TableName mvFqName = new TableName(null, ctx.getDatabase(), mvName);
            MaterializedViewPlus mvPlus = MaterializedViewPlus.of(mv, mvFqName);
            String mvSchema2 = mvPlus.getCreateMaterializedViewSql();
            System.out.println(mvSchema2);
            starRocksAssert.dropMaterializedView(mvName);

            mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(db)
                    .getTable(mvName);
            Assert.assertNull(mv);
            starRocksAssert.withMaterializedView(mvSchema2);
            mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(db)
                    .getTable(mvName);
            mvPlus = MaterializedViewPlus.of(mv, mvFqName);
            String mvSchema3 = mvPlus.getCreateMaterializedViewSql();
            Assert.assertEquals(mvSchema2, mvSchema3);
        }
    }

    @Test
    public void testGenerateSameNameMVQuery01() {
        String sql = TPCDSTestUtil.getQuery("query01");
        Map<String, String> mvMap = getMaterializedViews(sql);
        for (Map.Entry<String, String> entry : mvMap.entrySet()) {
            String mvName = entry.getKey();
            String mvSchema = entry.getValue();
            System.out.println("create:" + mvName);
            System.out.println(mvSchema);
        }
    }

    @Test
    public void testAppendStmt() {
        TablePlus table = PlanPieceInfo.getTable("_tunespace_", 1, 1);
        String sql = TPCDSTestUtil.getQuery("query01");
        QueryStatementPlus stmt = RboOptimizer.getQueryStatement(connectContext, sql);
        QueryStatement queryStmt = stmt.getQueryStatement();
        Map<String, FQTable> fqTableMap = stmt.getFqTableMap();
        List<OptExpression> subPlans =
                RboOptimizer.getSubPlans(queryStmt, ctx, PlanPiecePattern.getSPJG());
        List<PlanPieceInfo> pieceInfos = subPlans.stream()
                .map(subPlan -> PlanPieceInfo.from(subPlan, false, fqTableMap))
                .collect(Collectors.toList());
        for (PlanPieceInfo info : pieceInfos) {
            String insertSql = table.getInsertSql(ImmutableList.of(info));
            System.out.println(insertSql);
        }
    }

    @Test
    public void testParseRowFormat() throws Exception {
        String sql = TPCDSTestUtil.getQuery("query01");
        QueryStatementPlus queryStmtPlus = RboOptimizer.getQueryStatement(ctx, sql);
        QueryStatement queryStmt = queryStmtPlus.getQueryStatement();
        Map<String, FQTable> fqTableMap = queryStmtPlus.getFqTableMap();
        List<OptExpression> subPlans = RboOptimizer.getSubPlans(queryStmt, ctx, PlanPiecePattern.getSPJG());
        OptExpression subPlan = subPlans.get(0);
        ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
        Optional<AggregatePiece> optPlanPiece =
                PlanPieceBuilder.createPlanPiece(subPlan, idConverter, fqTableMap).cast(AggregatePiece.class);
        Preconditions.checkArgument(optPlanPiece.isPresent());
        AggregatePiece planPiece = optPlanPiece.get();
        PrettyPrinter traceLog = new PrettyPrinter();
        AggregatePolicy policy = AggregatePolicies.defaultPolicies(idConverter::nextId, traceLog);
        PlanPieceInfo pieceInfo = PlanPieceInfo.from(planPiece, policy, fqTableMap);
        TablePlus table = PlanPieceInfo.getTable("tunespace", 1, 1);
        String insertSql = table.getInsertSql(Collections.singletonList(pieceInfo));
        System.out.println(insertSql);
        List<ColumnPlus> columns = table.getColumnPluses();
        TRowFormat row = ColumnPlus.pack(pieceInfo, columns);
        TSerializer serializer = new TSerializer(TCompactProtocol::new);
        byte[] data = serializer.serialize(row);
        TDeserializer deserializer = new TDeserializer(TCompactProtocol::new);
        TRowFormat newRow = new TRowFormat();
        deserializer.deserialize(newRow, data);
        PlanPieceInfo newPieceInfo = ColumnPlus.unpack(PlanPieceInfo.class, columns, newRow);
        String newInsertSql = table.getInsertSql(Collections.singletonList(newPieceInfo));
        System.out.println(newInsertSql);
    }

    @Test
    public void recommendRollupCountDistinct() {
        String sql = "SELECT\n" +
                "  (count(distinct _ta0000.ss_quantity)) AS _ca0004\n" +
                "  ,_ta0000.ca_country\n" +
                "FROM\n" +
                "  (\n" +
                "    SELECT\n" +
                "      store_sales.ss_ext_wholesale_cost\n" +
                "      ,store_sales.ss_quantity\n" +
                "      ,store_sales.ss_ext_sales_price\n" +
                "      ,date_dim.d_year\n" +
                "      ,customer_address.ca_state\n" +
                "      ,customer_address.ca_country\n" +
                "      ,household_demographics.hd_dep_count\n" +
                "      ,customer_demographics.cd_marital_status\n" +
                "      ,customer_demographics.cd_education_status\n" +
                "      ,store_sales.ss_sales_price\n" +
                "      ,store_sales.ss_net_profit\n" +
                "    FROM\n" +
                "      store_sales\n" +
                "      INNER JOIN\n" +
                "      store\n" +
                "      ON (store.s_store_sk = store_sales.ss_store_sk)\n" +
                "      INNER JOIN\n" +
                "      customer_demographics\n" +
                "      ON (customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk)\n" +
                "      INNER JOIN\n" +
                "      household_demographics\n" +
                "      ON (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)\n" +
                "      INNER JOIN\n" +
                "      customer_address\n" +
                "      ON (store_sales.ss_addr_sk = customer_address.ca_address_sk)\n" +
                "      INNER JOIN\n" +
                "      date_dim\n" +
                "      ON (store_sales.ss_sold_date_sk = date_dim.d_date_sk)\n" +
                "  ) _ta0000\n" +
                "WHERE\n" +
                "  _ta0000.d_year between 1992 and 2023\n" +
                "  and _ta0000.ca_state = 'ASIA'\n" +
                "GROUP BY\n" +
                "  _ta0000.ca_country";
        Map<String, String> mvs = getMaterializedViews(sql);
        mvs.forEach((a, b) -> {
            System.out.println(b);
        });
    }

    @Test
    public void testTuneSpace0() throws IOException {
        String baseDir = "/home/grakra/workspace1/starrocks-concurrent-test/src/main/resources";
        //String baseDir = "./";

        File dir = new File(baseDir + "/tpcds_mv");
        File logDir = new File(baseDir + "/tpcds_mv_log");
        File errDir = new File(baseDir + "/tpcds_mv_err");

        if (logDir.exists()) {
            logDir.delete();
        }
        logDir.mkdirs();
        if (errDir.exists()) {
            errDir.delete();
        }
        errDir.mkdirs();
        if (dir.exists()) {
            dir.delete();
        }
        dir.mkdirs();

        TablePlus table = PlanPieceInfo.getTable("_auto_tuning_.tunespace", 8, 1);
        for (int i = 28; i <= 28; i++) {
            String suffix = "00" + i;
            suffix = suffix.substring(suffix.length() - 2);
            String name = "query" + suffix;
            String qName = name;
            String alterQName = name + "-1";
            File mvf = new File(dir, name + "_mv.sql");
            if (mvf.exists()) {
                mvf.delete();
            }
            File logf = new File(logDir, name + "_mv.log");
            if (logf.exists()) {
                logf.delete();
            }

            File errf = new File(errDir, name + "_mv.err");
            if (errf.exists()) {
                errf.delete();
            }
            PrintWriter logFile = new PrintWriter(new FileWriter(logf, true));

            try {
                String sql =
                        Optional.ofNullable(TPCDSTestUtil.getQuery(qName)).orElse(TPCDSTestUtil.getQuery(alterQName));

                ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
                Pair<Map<String, FQTable>, List<OptExpression>>
                        fQTableMapAndSubPlans = RboOptimizer.getSubPlans(sql, ctx, PlanPiecePattern.getSPJG());
                List<OptExpression> subPlans = fQTableMapAndSubPlans.second;
                Map<String, FQTable> fqTableMap = fQTableMapAndSubPlans.first;
                if (subPlans.isEmpty()) {
                    continue;
                }
                OptExpression subPlan = subPlans.get(0);
                PlanPiece planPiece = PlanPieceBuilder.createPlanPiece(subPlan, idConverter, fqTableMap);
                PrettyPrinter traceLog = new PrettyPrinter();
                AggregatePolicy policy = AggregatePolicies.defaultPolicies(idConverter::nextId, traceLog);
                PlanPieceInfo pieceInfo = PlanPieceInfo.from(planPiece, policy, fqTableMap);
                System.out.println(pieceInfo.getOriginalQuery());
                System.out.println(pieceInfo.getQuery());
                String insertSql = table.getInsertSql(Collections.singletonList(pieceInfo));
                System.out.println("[BEGIN] INSERT SQL==============");
                System.out.println(insertSql);
                System.out.println("[END] INSERT SQL==============");

                fQTableMapAndSubPlans = RboOptimizer.getSubPlans(sql, ctx, PlanPiecePattern.getSPJG());
                subPlans = fQTableMapAndSubPlans.second;
                fqTableMap = fQTableMapAndSubPlans.first;
                subPlan = subPlans.get(0);
                idConverter = new ColumnRefToIdConverter();
                planPiece = PlanPieceBuilder.createPlanPiece(subPlan, idConverter, fqTableMap);

                MVGenerateContext mvGenCxt = MVGenerateContext.builder()
                        .enableGenerateTraceLog()
                        .enablePolicyTraceLog()
                        .setMvNameGenerator(query -> MVName.generateFromQuery(query).toString())
                        .setNextId(idConverter::nextId)
                        .build();
                policy = AggregatePolicies.defaultPolicies(idConverter::nextId, traceLog);
                planPiece = policy.convert(planPiece.cast()).get();
                QueryGenerateResult result = AggregateMVGenerator.generate(planPiece.cast(), mvGenCxt);

                logFile.println("AggregatePolices=============");
                String traceInfos = traceLog.getResult();
                logFile.println(traceInfos);
                System.out.println(traceInfos);
                String planPieceStr = PlanPiecePrinter.print(planPiece);

                logFile.println("PlanPiece====================");
                logFile.println(planPieceStr);
                System.out.println("PlanPiece====================");
                System.out.println(planPieceStr);

                String mv = result.getSubquery().getResult();
                String traceLogs = result.getTraceLog().map(PrettyPrinter::getResult).orElse("");
                IOUtil.writeFile(mv, mvf);
                logFile.println("Trace Info==================");
                logFile.println(traceLogs);
                System.out.println("Trace Info ====================");
                System.out.println(traceLogs);
                System.out.println("MV Schema===================");
                System.out.println(mv);
                logFile.flush();
                logFile.close();
            } catch (Throwable ex) {
                ex.printStackTrace();
                logFile.flush();
                logFile.close();
                logf.renameTo(errf);
            }
        }
    }

    @Test
    public void testSPJG0() throws Exception {

        String baseDir = "/home/grakra/workspace1/starrocks-concurrent-test/src/main/resources";
        //String baseDir = "./";

        File dir = new File(baseDir + "/tpcds_mv");
        File logDir = new File(baseDir + "/tpcds_mv_log");
        File errDir = new File(baseDir + "/tpcds_mv_err");

        if (logDir.exists()) {
            logDir.delete();
        }
        logDir.mkdirs();
        if (errDir.exists()) {
            errDir.delete();
        }
        errDir.mkdirs();
        if (dir.exists()) {
            dir.delete();
        }
        dir.mkdirs();

        for (int i = 1; i < 2; i++) {
            String suffix = "00" + i;
            suffix = suffix.substring(suffix.length() - 2);
            String name = "query" + suffix;
            String qName = name;
            String alterQName = name + "-1";
            File mvf = new File(dir, name + "_mv.sql");
            if (mvf.exists()) {
                mvf.delete();
            }
            File logf = new File(logDir, name + "_mv.log");
            if (logf.exists()) {
                logf.delete();
            }

            File errf = new File(errDir, name + "_mv.err");
            if (errf.exists()) {
                errf.delete();
            }
            PrintWriter logFile = new PrintWriter(new FileWriter(logf, true));

            try {
                String sql =
                        Optional.ofNullable(TPCDSTestUtil.getQuery(qName)).orElse(TPCDSTestUtil.getQuery(alterQName));
                Pair<Map<String, FQTable>, List<OptExpression>> fqTableMapAndSubPlans =
                        RboOptimizer.getSubPlans(sql, ctx, PlanPiecePattern.getSPJG());
                Map<String, FQTable> fqTableMap = fqTableMapAndSubPlans.first;
                List<OptExpression> subPlans = fqTableMapAndSubPlans.second;
                for (OptExpression optExp : subPlans) {
                    String subPlan = LogicalPlanPrinter.print(optExp, true, true);
                    logFile.println("SubPlan===================");
                    logFile.println(subPlan);
                    System.out.println("SubPlan===================");
                    System.out.println(subPlan);
                }
                //System.out.println(UtFrameUtils.getVerboseFragmentPlan(ctx, sql));
                if (subPlans.isEmpty()) {
                    continue;
                }
                ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
                PlanPiece planPiece = PlanPieceBuilder.createPlanPiece(subPlans.get(0), idConverter, fqTableMap);
                AggregatePiece aggPiece = planPiece.cast();
                PrettyPrinter traceLog = new PrettyPrinter();
                planPiece = AggregatePolicies.defaultPolicies(idConverter::nextId, traceLog).convert(aggPiece)
                        .orElse(aggPiece);
                logFile.println("AggregatePolices=============");
                String traceInfos = traceLog.getResult();
                logFile.println(traceInfos);
                System.out.println(traceInfos);
                String planPieceStr = PlanPiecePrinter.print(planPiece);
                logFile.println("PlanPiece====================");
                logFile.println(planPieceStr);
                System.out.println("PlanPiece====================");
                System.out.println(planPieceStr);
                MVGenerateContext context = MVGenerateContext.builder()
                        .enableGenerateTraceLog()
                        .enablePolicyTraceLog()
                        .setNextId(idConverter::nextId)
                        .setMvNameGenerator(query -> MVName.generateFromQuery(query).toString()).build();
                QueryGenerateResult result = AggregateMVGenerator.generate(planPiece.cast(), context);
                String mv = result.getSubquery().getResult();
                String traceLogs = result.getTraceLog().map(PrettyPrinter::getResult).orElse("");
                IOUtil.writeFile(mv, mvf);
                logFile.println("Trace Info==================");
                logFile.println(traceLogs);
                System.out.println("Trace Info ====================");
                System.out.println(traceLogs);
                System.out.println("MV Schema===================");
                System.out.println(mv);
                logFile.flush();
                logFile.close();
            } catch (Throwable ex) {
                ex.printStackTrace();
                logFile.flush();
                logFile.close();
                logf.renameTo(errf);
            }
        }
    }

    @Test
    public void test() {
        PrettyPrinter p0 = new PrettyPrinter();
        List<PrettyPrinter> printers = Stream.of("a", "b", "c").map(item -> {
            PrettyPrinter p = new PrettyPrinter();
            p.add(item);
            return p;
        }).collect(Collectors.toList());
        p0.add("items=").add("{").newLine();
        p0.indentEnclose(() -> {
            p0.addSuperStepsWithNl(",", printers);
        });
        p0.newLine().add("}").newLine();
        System.out.println(p0.getResult());
    }

    @Test
    public void testTuneSpace() {
        TablePlus table = PlanPieceInfo.getTable("_auto_tuning_.tunespace", 8, 1);
        PlanPieceInfo info = new PlanPieceInfo();
        info.setId(1);
        info.setQuery("select * from t0");
        info.setTs(new Timestamp(System.currentTimeMillis()));
        info.setOriginalQuery("select * from t1");
        info.setCategory(PlanPieceInfo.Category.MV);
        //PlanPieceInfo.
        PieceTraits traits = new PieceTraits();
        traits.setVersion(1);
        traits.setNumMetrics(10);
        info.setTraits(traits);
        System.out.println(table.getCreateTableSql());
        System.out.println(table.getInsertSql(ImmutableList.of(info)));
    }

    @Test
    public void testEscape() {
        String[] ss = new String[] {
                "\"",
                "\"\\",
                "'",
                "'",
                "\n",
                "abc\"def",
                "\"abc\"def",
                "\"abc\"def",
                "\"abc\"def",
                "\"abc\"def",
                "\"abc\"def",
                "\"abc\\\\\n\"def",
        };
        for (String s : ss) {
            System.out.printf("%s=>%s\n", s, PrettyPrinter.escapedDoubleQuoted(s).getResult());
            System.out.printf("%s=>%s\n", s, PrettyPrinter.escapedSingleQuoted(s).getResult());
        }
    }

    @Test
    public void testTunespaceOperations() {
        StatementBase stmt = RboOptimizer.parseAndAnalyze(ctx, "CREATE TUNESPACE _tunespace_");
        System.out.println(stmt);
    }
}
