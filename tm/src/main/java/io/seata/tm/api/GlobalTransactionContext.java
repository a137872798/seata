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
package io.seata.tm.api;

import io.seata.core.context.RootContext;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.GlobalStatus;

/**
 * GlobalTransaction API
 * 全局事务上下文
 * @author sharajava
 */
public class GlobalTransactionContext {

    private GlobalTransactionContext() {
    }

    /**
     * Try to create a new GlobalTransaction.
     *
     * @return
     */
    private static GlobalTransaction createNew() {
        GlobalTransaction tx = new DefaultGlobalTransaction();
        return tx;
    }

    /**
     * Get GlobalTransaction instance bind on current thread.
     * 获取当前线程绑定的 上下文对象
     * @return null if no transaction context there.
     */
    private static GlobalTransaction getCurrent() {
        String xid = RootContext.getXID();
        if (xid == null) {
            return null;
        }
        return new DefaultGlobalTransaction(xid, GlobalStatus.Begin, GlobalTransactionRole.Participant);
    }

    /**
     * Get GlobalTransaction instance bind on current thread. Create a new on if no existing there.
     * 获取当前线程绑定的全局事务对象
     * @return new context if no existing there.
     */
    public static GlobalTransaction getCurrentOrCreate() {
        GlobalTransaction tx = getCurrent();
        if (tx == null) {
            // 如果当前事务id 还没有创建 那么全局事务还没有生成 这里就要创建一个新的 全局事务对象
            return createNew();
        }
        return tx;
    }

    /**
     * Reload GlobalTransaction instance according to the given XID
     *
     * @param xid the xid
     * @return reloaded transaction instance.
     * @throws TransactionException the transaction exception
     */
    public static GlobalTransaction reload(String xid) throws TransactionException {
        GlobalTransaction tx = new DefaultGlobalTransaction(xid, GlobalStatus.UnKnown, GlobalTransactionRole.Launcher) {
            @Override
            public void begin(int timeout, String name) throws TransactionException {
                throw new IllegalStateException("Never BEGIN on a RELOADED GlobalTransaction. ");
            }
        };
        return tx;
    }
}
