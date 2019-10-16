/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.server.session;

import io.seata.core.exception.TransactionException;
import io.seata.core.model.BranchType;
import io.seata.core.model.GlobalStatus;
import io.seata.server.UUIDGenerator;

/**
 * The type Session helper.
 * helper 对象
 * @author sharajava
 */
public class SessionHelper {

    private SessionHelper() {}

    /**
     * 基于某个 globalSession 创建一个新的 branchSession 对象
     * @param globalSession
     * @param branchType
     * @param resourceId
     * @param lockKeys
     * @param clientId
     * @return
     */
    public static BranchSession newBranchByGlobal(GlobalSession globalSession, BranchType branchType, String resourceId, String lockKeys, String clientId) {
        return newBranchByGlobal(globalSession, branchType, resourceId, null, lockKeys, clientId);
    }

    /**
     * New branch by global branch session.
     *
     * @param globalSession the global session  全局事务
     * @param branchType    the branch type   分事务类型
     * @param resourceId    the resource id   分事务唯一标识
     * @param lockKeys      the lock keys    锁住的字段
     * @param clientId      the client id    对应客户端id
     * @return the branch session
     */
    public static BranchSession newBranchByGlobal(GlobalSession globalSession, BranchType branchType, String resourceId,
            String applicationData, String lockKeys, String clientId) {
        // 创建分事务对象
        BranchSession branchSession = new BranchSession();

        branchSession.setXid(globalSession.getXid());
        branchSession.setTransactionId(globalSession.getTransactionId());
        // branchId 使用UUID 生成
        branchSession.setBranchId(UUIDGenerator.generateUUID());
        branchSession.setBranchType(branchType);
        branchSession.setResourceId(resourceId);
        branchSession.setLockKey(lockKeys);
        branchSession.setClientId(clientId);
        branchSession.setApplicationData(applicationData);

        return branchSession;
    }

    /**
     * End committed.
     * 当 commit 完成后执行
     * @param globalSession the global session
     * @throws TransactionException the transaction exception
     */
    public static void endCommitted(GlobalSession globalSession) throws TransactionException {
        globalSession.changeStatus(GlobalStatus.Committed);
        globalSession.end();
    }

    /**
     * End commit failed.
     * 当commit 完成后 执行
     * @param globalSession the global session
     * @throws TransactionException the transaction exception
     */
    public static void endCommitFailed(GlobalSession globalSession) throws TransactionException {
        globalSession.changeStatus(GlobalStatus.CommitFailed);
        globalSession.end();
    }

    /**
     * End rollbacked.
     *
     * @param globalSession the global session
     * @throws TransactionException the transaction exception
     */
    public static void endRollbacked(GlobalSession globalSession) throws TransactionException {
        GlobalStatus currentStatus = globalSession.getStatus();
        if (isTimeoutGlobalStatus(currentStatus)) {
            globalSession.changeStatus(GlobalStatus.TimeoutRollbacked);
        } else {
            globalSession.changeStatus(GlobalStatus.Rollbacked);
        }
        globalSession.end();
    }

    /**
     * End rollback failed.
     *
     * @param globalSession the global session
     * @throws TransactionException the transaction exception
     */
    public static void endRollbackFailed(GlobalSession globalSession) throws TransactionException {
        GlobalStatus currentStatus = globalSession.getStatus();
        if (isTimeoutGlobalStatus(currentStatus)) {
            globalSession.changeStatus(GlobalStatus.TimeoutRollbackFailed);
        } else {
            globalSession.changeStatus(GlobalStatus.RollbackFailed);
        }
        globalSession.end();
    }

    public static boolean isTimeoutGlobalStatus(GlobalStatus status) {
        return status == GlobalStatus.TimeoutRollbacked
                || status == GlobalStatus.TimeoutRollbackFailed
                || status == GlobalStatus.TimeoutRollbacking
                || status == GlobalStatus.TimeoutRollbackRetrying;
    }
}
