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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import kotlin.text.Charsets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;

public class TablePruningCTETest extends TablePruningTestBase {
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
        starRocksAssert.withDatabase("tpch").useDatabase("tpch");
        getTPCHCreateTableSqlList().forEach(createTblSql -> {
            try {
                starRocksAssert.withTable(createTblSql);
                starRocksAssert.withTable(replaceTableName(createTblSql));
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });

        getSqlList("sql/tpch_pk_tables/", "AddFKConstraints")
                .forEach(fkConstraints -> Arrays.stream(fkConstraints.split("\n")).forEach(addFk ->
                        {
                            try {
                                starRocksAssert.alterTableProperties(addFk);
                            } catch (Exception e) {
                                e.printStackTrace();
                                Assert.fail();
                            }
                        }
                ));

        String[][] tpchShowAndResults = new String[][] {
                {"nation",
                        "\"foreign_key_constraints\" = \"(n_regionkey) REFERENCES default_catalog.tpch.region(r_regionkey)\",\n"
                },
                {"supplier",
                        "\"foreign_key_constraints\" = \"(s_nationkey) REFERENCES default_catalog.tpch.nation(n_nationkey)\",\n"
                },
                {"customer",
                        "\"foreign_key_constraints\" = \"(c_nationkey) REFERENCES default_catalog.tpch.nation(n_nationkey)\""
                },
                {"partsupp",
                        "\"foreign_key_constraints\" = \"(ps_partkey) REFERENCES default_catalog.tpch.part(p_partkey);(ps_suppkey) REFERENCES default_catalog.tpch.supplier(s_suppkey)\",\n"},
                {"orders",
                        "\"foreign_key_constraints\" = \"(o_custkey) REFERENCES default_catalog.tpch.customer(c_custkey)\",\n"},
                {"lineitem",
                        "\"foreign_key_constraints\" = \"(l_orderkey) REFERENCES default_catalog.tpch.orders(o_orderkey);(l_partkey) REFERENCES default_catalog.tpch.part(p_partkey);(l_suppkey) REFERENCES default_catalog.tpch.supplier(s_suppkey);(l_partkey,l_suppkey) REFERENCES default_catalog.tpch.partsupp(ps_partkey,ps_suppkey)\",\n"}
        };
        for (String[] showAndResult : tpchShowAndResults) {
            List<List<String>> res = starRocksAssert.show(String.format("show create table %s", showAndResult[0]));
            Assert.assertTrue(res.size() >= 1 && res.get(0).size() >= 2);
            String createTableSql = res.get(0).get(1);
            Assert.assertTrue(createTableSql, createTableSql.contains(showAndResult[1]));
        }
        FeConstants.runningUnitTest = true;
    }

    private static String replaceTableName(String createTableSql) {
        Pattern pat = Pattern.compile("(?i)^\\s*CREATE\\s*TABLE\\s*`?(\\w+)`?");
        return createTableSql.replaceAll(pat.pattern(), "CREATE TABLE `$10`");
    }

    @Test
    public void testUpdate() {
        String sql = "WITH cte0 as (\n" +
                "WITH cte1 as (\n" +
                "select l_suppkey as suppkey,\n" +
                "       l_partkey as partkey, \n" +
                "       avg(l_tax) as tax\n" +
                "from lineitem\n" +
                "group by partkey, suppkey\n" +
                "),\n" +
                "\n" +
                "cte2 as (\n" +
                "select \n" +
                "\tlineitem.l_tax as tax,\n" +
                "\tcte1.tax as avg_tax,\n" +
                "\tlineitem.l_shipdate as shipdate,\n" +
                "\tlineitem.l_orderkey as orderkey,\n" +
                "\tlineitem.l_partkey as partkey,\n" +
                "\tlineitem.l_suppkey as suppkey\n" +
                "from \n" +
                "lineitem left join cte1 on lineitem.l_suppkey = cte1.suppkey and lineitem.l_partkey = cte1.partkey\n" +
                ")\n" +
                "\n" +
                "select  shipdate,\n" +
                "\torderkey,\n" +
                "\tpartkey,\n" +
                "\tsuppkey,\n" +
                "\t(case when tax = 0 then 0 else avg_tax end) as tax\n" +
                "from cte2     \n" +
                ")\n" +
                "update lineitem set l_tax = cte0.tax from cte0\n" +
                "where\n" +
                "   lineitem.l_orderkey = cte0.orderkey and \n" +
                "   lineitem.l_partkey = cte0.partkey and \n" +
                "   lineitem.l_suppkey = cte0.suppkey";

        System.out.println(sql);
        checkHashJoinCountWithOnlyRBO(sql, 1);
    }
    @Test
    public void testUpdate1(){
        String q = getSqlList("sql/tpch_pk_tables/", "q1").get(0);
        checkHashJoinCountWithOnlyRBO(q, 3);
    }

}