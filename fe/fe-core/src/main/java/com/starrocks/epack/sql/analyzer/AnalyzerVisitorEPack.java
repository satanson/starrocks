// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.AlterSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.CreateRoleMappingStatement;
import com.starrocks.epack.sql.ast.CreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.DropRoleMappingStatement;
import com.starrocks.epack.sql.ast.DropSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzerVisitor;

public class AnalyzerVisitorEPack extends AnalyzerVisitor {

    // ---------------------------------------- Security Policy Statement ------------------------------------------

    @Override
    public Void visitCreatePolicyStatement(CreatePolicyStmt stmt, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(stmt, context);
        return null;
    }

    @Override
    public Void visitDropPolicyStatement(DropPolicyStmt stmt, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(stmt, context);
        return null;
    }

    @Override
    public Void visitAlterPolicyStatement(AlterPolicyStmt stmt, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(stmt, context);
        return null;
    }

    @Override
    public Void visitShowPolicyStatement(ShowPolicyStmt stmt, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(stmt, context);
        return null;
    }

    @Override
    public Void visitShowCreatePolicyStatement(ShowCreatePolicyStmt stmt, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(stmt, context);
        return null;
    }

    // ---------------------------------------- Security Integration Statement -------------------------------------

    @Override
    public Void visitCreateSecurityIntegrationStatement(CreateSecurityIntegrationStatement statement,
                                                        ConnectContext context) {
        SecurityIntegrationStatementAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitAlterSecurityIntegrationStatement(AlterSecurityIntegrationStatement statement,
                                                       ConnectContext context) {
        SecurityIntegrationStatementAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitDropSecurityIntegrationStatement(DropSecurityIntegrationStatement statement,
                                                      ConnectContext context) {
        SecurityIntegrationStatementAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitShowCreateSecurityIntegrationStatement(ShowCreateSecurityIntegrationStatement statement,
                                                            ConnectContext context) {
        SecurityIntegrationStatementAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitCreateRoleMappingStatement(CreateRoleMappingStatement statement, ConnectContext context) {
        RoleMappingStatementAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitDropRoleMappingStatement(DropRoleMappingStatement statement,
                                              ConnectContext context) {
        RoleMappingStatementAnalyzer.analyze(statement, context);
        return null;
    }

}
