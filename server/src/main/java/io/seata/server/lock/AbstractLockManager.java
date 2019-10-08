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
package io.seata.server.lock;

import java.util.ArrayList;
import java.util.List;

import io.seata.common.XID;
import io.seata.common.util.StringUtils;
import io.seata.core.lock.RowLock;
import io.seata.server.session.BranchSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Abstract lock manager.
 * LM 对象 具备 加锁和 解锁的功能
 * @author zhangsen
 * @data 2019 /4/25
 */
public abstract class AbstractLockManager implements LockManager {

    /**
     * The constant LOGGER.
     */
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractLockManager.class);

    /**
     * Collect row locks list.`
     *
     * @param branchSession the branch session
     * @return the list
     */
    protected List<RowLock> collectRowLocks(BranchSession branchSession) {
        List<RowLock> locks = new ArrayList<>();
        if (branchSession == null || StringUtils.isBlank(branchSession.getLockKey())) {
            return locks;
        }
        String xid = branchSession.getXid();
        // 获取该 分支的 唯一标识
        String resourceId = branchSession.getResourceId();
        long transactionId = branchSession.getTransactionId();

        // 获取锁字段
        String lockKey = branchSession.getLockKey();

        // 收集需要上锁的 行
        return collectRowLocks(lockKey, resourceId, xid, transactionId, branchSession.getBranchId());
    }

    /**
     * Collect row locks list.
     *
     * @param lockKey    the lock key
     * @param resourceId the resource id
     * @param xid        the xid
     * @return the list
     */
    protected List<RowLock> collectRowLocks(String lockKey, String resourceId, String xid) {
        return collectRowLocks(lockKey, resourceId, xid, XID.getTransactionId(xid), null);
    }

    /**
     * Collect row locks list.
     * 收集需要上锁的行
     * @param lockKey       the lock key
     * @param resourceId    the resource id
     * @param xid           the xid
     * @param transactionId the transaction id
     * @param branchID      the branch id
     * @return the list
     */
    protected List<RowLock> collectRowLocks(String lockKey, String resourceId, String xid, Long transactionId,
                                            Long branchID) {
        List<RowLock> locks = new ArrayList<RowLock>();

        String[] tableGroupedLockKeys = lockKey.split(";");
        for (String tableGroupedLockKey : tableGroupedLockKeys) {
            int idx = tableGroupedLockKey.indexOf(":");
            if (idx < 0) {
                return locks;
            }
            // :前面的代表表名
            String tableName = tableGroupedLockKey.substring(0, idx);
            // 后面部分代表 主键
            String mergedPKs = tableGroupedLockKey.substring(idx + 1);
            if (StringUtils.isBlank(mergedPKs)) {
                return locks;
            }
            // 主键通过 , 拼接 这里是拆分他们
            String[] pks = mergedPKs.split(",");
            if (pks == null || pks.length == 0) {
                return locks;
            }
            for (String pk : pks) {
                if (StringUtils.isNotBlank(pk)) {
                    // 为每个主键生成 lock 对象
                    RowLock rowLock = new RowLock();
                    rowLock.setXid(xid);
                    rowLock.setTransactionId(transactionId);
                    rowLock.setBranchId(branchID);
                    rowLock.setTableName(tableName);
                    rowLock.setPk(pk);
                    rowLock.setResourceId(resourceId);
                    locks.add(rowLock);
                }
            }
        }
        return locks;
    }

}
