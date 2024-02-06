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

package com.starrocks.sql.automv.qe;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.LogUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.automv.generator.ColumnRefToIdConverter;
import com.starrocks.sql.automv.pattern.PlanPiecePattern;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.PlanPieceBuilder;
import com.starrocks.sql.automv.policies.AggregatePolicies;
import com.starrocks.sql.automv.policies.AggregatePolicy;
import com.starrocks.sql.automv.tunespace.CollectAstVisitor;
import com.starrocks.sql.automv.tunespace.QueryStatementPlus;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerConfig;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerTraceInfo;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.transformation.MergeProjectWithChildRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoProjectRule;
import com.starrocks.sql.optimizer.task.RewriteTreeTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.parser.SqlParser;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RboOptimizer {
    OptimizerConfig optimizerConfig;
    OptimizerContext optimizerContext;
    TaskContext taskContext;
    OptExpression tree;

    public RboOptimizer(LogicalPlan logicalPlan, ColumnRefFactory columnRefFactory, ConnectContext connectContext) {
        optimizerConfig = new OptimizerConfig(OptimizerConfig.OptimizerAlgorithm.RULE_BASED);
        optimizerContext = new OptimizerContext(null, columnRefFactory, connectContext, optimizerConfig);
        // CTE must be inlined to extract sub-queries
        optimizerContext.getCteContext().setEnableCTE(false);
        OptimizerTraceInfo traceInfo = new OptimizerTraceInfo(connectContext.getQueryId(), null);
        optimizerContext.setTraceInfo(traceInfo);
        taskContext =
                new TaskContext(optimizerContext, new PhysicalPropertySet(),
                        new ColumnRefSet(logicalPlan.getOutputColumn()),
                        Double.MAX_VALUE);
        tree = logicalPlan.getRoot();
        if (!tree.getOp().getOpType().equals(OperatorType.LOGICAL)) {
            tree = OptExpression.create(new LogicalTreeAnchorOperator(), tree);
        }
    }

    private static void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }

        ExpressionContext context = new ExpressionContext(root);
        context.deriveLogicalProperty();
        root.setLogicalProperty(context.getRootProperty());
    }

    public static OptExpression getLogicalPlan(QueryStatement queryStmt, ConnectContext connectContext) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        QueryRelation query = queryStmt.getQueryRelation();
        LogicalPlan logicalPlan =
                new RelationTransformer(columnRefFactory, connectContext).transformWithSelectLimit(query);
        RboOptimizer optimizer = new RboOptimizer(logicalPlan, columnRefFactory, connectContext);
        return optimizer.optimize();
    }

    public static List<OptExpression> getSubPlans(QueryStatement queryStmt, ConnectContext connectContext,
                                                  PlanPiecePattern pattern) {
        return PlanPiecePattern.extract(getLogicalPlan(queryStmt, connectContext), pattern);
    }

    public static Optional<OptExpression> getEntirePlan(QueryStatement queryStmt, ConnectContext connectContext,
                                                        PlanPiecePattern pattern) {
        return PlanPiecePattern.extractEntire(getLogicalPlan(queryStmt, connectContext), pattern);
    }

    public static StatementBase parseAndAnalyze(ConnectContext connectContext, String query) {
        query = LogUtil.removeLineSeparator(query);
        List<StatementBase> statements = SqlParser.parse(query, connectContext.getSessionVariable());
        SessionVariable oldSessionVariable = connectContext.getSessionVariable();
        StatementBase statementBase = statements.get(0);
        try {
            // update session variable by adding optional hints.
            Optional<Map<String, String>> optHints = Util.downcast(statementBase, QueryStatement.class)
                    .flatMap(queryStmt -> Util.downcast(queryStmt.getQueryRelation(), SelectRelation.class))
                    .flatMap(selectRelation -> Optional.ofNullable(selectRelation.getSelectList().getOptHints()));

            if (optHints.isPresent()) {
                Map<String, String> hints = optHints.get();
                SessionVariable sessionVariable = (SessionVariable) oldSessionVariable.clone();
                for (String key : hints.keySet()) {
                    VariableMgr.setSystemVariable(sessionVariable,
                            new SystemVariable(key, new StringLiteral(hints.get(key))), true);
                }
                connectContext.setSessionVariable(sessionVariable);
            }
            Analyzer.analyze(statementBase, connectContext);
            return statementBase;
        } catch (DdlException e) {
            throw new RuntimeException(e);
        } finally {
            connectContext.setSessionVariable(oldSessionVariable);
        }
    }

    public static QueryStatementPlus collectFQTables(QueryStatement queryStmt, ConnectContext connectContext) {
        return CollectAstVisitor.collectFQTables(queryStmt, connectContext);
    }

    private static Function<OptExpression, PlanPiece> subPlanToPiece(Map<String, FQTable> fqTableMap) {
        return subPlan -> {
            ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
            Optional<AggregatePiece> optPlanPiece =
                    PlanPieceBuilder.createPlanPiece(subPlan, idConverter, fqTableMap).cast(AggregatePiece.class);
            Preconditions.checkArgument(optPlanPiece.isPresent());
            PrettyPrinter policyTrace = new PrettyPrinter();
            AggregatePolicy policy = AggregatePolicies.defaultPolicies(idConverter::nextId, policyTrace);
            AggregatePiece newAggPiece = Objects.requireNonNull(policy.convert(optPlanPiece.get()).orElse(null));
            System.out.println(policyTrace.getResult());
            return newAggPiece;
        };
    }

    public static List<PlanPiece> getPlanPieces(String sql, ConnectContext ctx) {
        QueryStatementPlus stmt = RboOptimizer.getQueryStatement(ctx, sql);
        QueryStatement queryStmt = stmt.getQueryStatement();
        Map<String, FQTable> fqTableMap = stmt.getFqTableMap();
        Function<OptExpression, PlanPiece> subPlanToPieceConverter = subPlanToPiece(fqTableMap);
        return RboOptimizer.getSubPlans(queryStmt, ctx, PlanPiecePattern.getSPJG())
                .stream()
                .map(subPlanToPieceConverter)
                .collect(Collectors.toList());
    }

    public static QueryStatementPlus getQueryStatement(ConnectContext connectContext, String query) {
        StatementBase stmt = parseAndAnalyze(connectContext, query);
        Preconditions.checkArgument(stmt instanceof QueryStatement);
        return collectFQTables((QueryStatement) stmt, connectContext);
    }

    public static Pair<Map<String, FQTable>, List<OptExpression>> getSubPlans(String query,
                                                                              ConnectContext connectContext,
                                                                              PlanPiecePattern pattern) {
        QueryStatementPlus queryStmt = getQueryStatement(connectContext, query);
        List<OptExpression> subPlans = getSubPlans(queryStmt.getQueryStatement(), connectContext, pattern);
        return Pair.create(queryStmt.getFqTableMap(), subPlans);
    }

    public RboOptimizer applyRules(RuleSetType ruleSet) {
        List<Rule> rules = RuleSet.getRewriteRulesByType(ruleSet);
        optimizerContext.getTaskScheduler()
                .pushTask(new RewriteTreeTask(taskContext, tree, rules, false));
        optimizerContext.getTaskScheduler().executeTasks(taskContext);
        deriveLogicalProperty(tree);
        return this;
    }

    public RboOptimizer applyRules(Rule... rules) {
        for (Rule rule : rules) {
            optimizerContext.getTaskScheduler()
                    .pushTask(new RewriteTreeTask(taskContext, tree, Collections.singletonList(rule), false));
            optimizerContext.getTaskScheduler().executeTasks(taskContext);
            deriveLogicalProperty(tree);
        }
        return this;
    }

    public OptExpression optimize() {
        return applyRules(RuleSetType.INLINE_CTE)
                .applyRules(RuleSetType.PUSH_DOWN_SUBQUERY)
                .applyRules(RuleSetType.SUBQUERY_REWRITE_COMMON)
                .applyRules(RuleSetType.SUBQUERY_REWRITE_TO_WINDOW)
                .applyRules(RuleSetType.SUBQUERY_REWRITE_TO_JOIN)
                .applyRules(RuleSetType.PUSH_DOWN_PREDICATE)
                .applyRules(new MergeTwoProjectRule())
                .applyRules(new MergeProjectWithChildRule())
                .getPlan();
    }

    public OptExpression getPlan() {
        return tree;
    }
}
