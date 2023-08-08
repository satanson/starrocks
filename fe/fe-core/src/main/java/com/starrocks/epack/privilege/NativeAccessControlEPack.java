// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.common.collect.Lists;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.NativeAccessControl;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.sql.ast.UserIdentity;

import java.util.List;
import java.util.Set;

public class NativeAccessControlEPack extends NativeAccessControl implements AccessControlEPack {
    @Override
    public void checkPolicyAction(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                  String db, String policy, PrivilegeType privilegeType) {
        List<String> objectTokens = Lists.newArrayList(catalogName, db, policy);
        ObjectType objectType = policyType.equals(PolicyType.MASKING) ? ObjectTypeEPack.MASKING_POLICY :
                ObjectTypeEPack.ROW_ACCESS_POLICY;
        if (!checkObjectTypeAction(currentUser, roleIds, privilegeType, objectType, objectTokens)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), objectType, policy);
        }
    }

    @Override
    public void checkAnyActionOnPolicy(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                       String db, String policy) {
        List<String> objectTokens = Lists.newArrayList(catalogName, db, policy);
        ObjectType objectType = policyType.equals(PolicyType.MASKING) ? ObjectTypeEPack.MASKING_POLICY :
                ObjectTypeEPack.ROW_ACCESS_POLICY;
        if (!checkAnyActionOnObject(currentUser, roleIds, objectType, objectTokens)) {
            AccessDeniedException.reportAccessDenied("ANY", objectType, policy);
        }
    }
}
