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

import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.core.context.RootContext;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.GlobalStatus;
import io.seata.core.model.TransactionManager;
import io.seata.tm.TransactionManagerHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Default global transaction.
 * 全局事务对象  该对象负责开启事务 回滚事务等
 * @author sharajava
 */
public class DefaultGlobalTransaction implements GlobalTransaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultGlobalTransaction.class);

    private static final int DEFAULT_GLOBAL_TX_TIMEOUT = 60000;

    private static final String DEFAULT_GLOBAL_TX_NAME = "default";

    /**
     * TM 对象
     */
    private TransactionManager transactionManager;

    /**
     * 全局事务id
     */
    private String xid;

    /**
     * 全局事务状态
     */
    private GlobalStatus status;

    /**
     * 代表发起者还是参与者
     */
    private GlobalTransactionRole role;

    /**
     * Instantiates a new Default global transaction.
     * 没有 xid 代表创建该对象的是全局事务的发起者
     */
    DefaultGlobalTransaction() {
        this(null, GlobalStatus.UnKnown, GlobalTransactionRole.Launcher);
    }

    /**
     * Instantiates a new Default global transaction.
     *
     * @param xid    the xid
     * @param status the status
     * @param role   the role
     */
    DefaultGlobalTransaction(String xid, GlobalStatus status, GlobalTransactionRole role) {
        this.transactionManager = TransactionManagerHolder.get();
        this.xid = xid;
        this.status = status;
        this.role = role;
    }

    @Override
    public void begin() throws TransactionException {
        begin(DEFAULT_GLOBAL_TX_TIMEOUT);
    }

    @Override
    public void begin(int timeout) throws TransactionException {
        begin(timeout, DEFAULT_GLOBAL_TX_NAME);
    }

    /**
     * 使用规定的超时时间 和事务名称开启事务
     * @param timeout Given timeout in MILLISECONDS.
     * @param name    Given name.  事务名称
     * @throws TransactionException
     */
    @Override
    public void begin(int timeout, String name) throws TransactionException {
        // 如果是 事务发起者 将信息提交到TC 上 如果是参与者就是确保 xid 已经绑定在当前线程中
        if (role != GlobalTransactionRole.Launcher) {
            check();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Ignore Begin(): just involved in global transaction [" + xid + "]");
            }
            return;
        }

        // 进入到这里代表是参与者
        if (xid != null) {
            throw new IllegalStateException();
        }
        // 此时上下文还不能绑定 xid
        if (RootContext.getXID() != null) {
            throw new IllegalStateException();
        }
        // 通过TM 开启事务 TM 又会通知 TC  这里会返回全局事务id
        xid = transactionManager.begin(null, null, name, timeout);
        // 代表事务已经开启 默认是 Unknown
        status = GlobalStatus.Begin;
        // 在本线程上绑定xid
        RootContext.bind(xid);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Begin new global transaction [" + xid + "]");
        }

    }

    /**
     * 提交全局事务  这里的逻辑跟 rollback 很接近
     * @throws TransactionException
     */
    @Override
    public void commit() throws TransactionException {
        // 只有发起者能提交
        if (role == GlobalTransactionRole.Participant) {
            // Participant has no responsibility of committing
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Ignore Commit(): just involved in global transaction [" + xid + "]");
            }
            return;
        }
        if (xid == null) {
            throw new IllegalStateException();
        }

        status = transactionManager.commit(xid);
        if (RootContext.getXID() != null) {
            if (xid.equals(RootContext.getXID())) {
                RootContext.unbind();
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("[" + xid + "] commit status:" + status);
        }

    }

    /**
     * 通过 TM 对象 发送本全局事务id 通过TC 调配回滚全局事务
     * @throws TransactionException
     */
    @Override
    public void rollback() throws TransactionException {
        // 如果当前角色是 参与者 不允许回滚
        if (role == GlobalTransactionRole.Participant) {
            // Participant has no responsibility of committing
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Ignore Rollback(): just involved in global transaction [" + xid + "]");
            }
            return;
        }
        if (xid == null) {
            throw new IllegalStateException();
        }

        // 由发起者进行回滚
        status = transactionManager.rollback(xid);
        if (RootContext.getXID() != null) {
            if (xid.equals(RootContext.getXID())) {
                RootContext.unbind();
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("[" + xid + "] rollback status:" + status);
        }
    }

    @Override
    public GlobalStatus getStatus() throws TransactionException {
        if (xid == null) {
            return GlobalStatus.UnKnown;
        }
        status = transactionManager.getStatus(xid);
        return status;
    }

    @Override
    public String getXid() {
        return xid;
    }

    /**
     * 不是发起者的话 必须确保 存在事务id
     */
    private void check() {
        if (xid == null) {
            throw new ShouldNeverHappenException();
        }

    }
}
