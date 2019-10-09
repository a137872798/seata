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
package io.seata.rm.datasource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import com.alibaba.druid.util.JdbcConstants;

import io.seata.common.util.StringUtils;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.exception.TransactionException;
import io.seata.core.exception.TransactionExceptionCode;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.BranchType;
import io.seata.rm.DefaultResourceManager;
import io.seata.rm.datasource.exec.LockConflictException;
import io.seata.rm.datasource.exec.LockRetryController;
import io.seata.rm.datasource.undo.SQLUndoLog;
import io.seata.rm.datasource.undo.UndoLogManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Connection proxy.
 * 连接代理对象
 * @author sharajava
 */
public class ConnectionProxy extends AbstractConnectionProxy {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProxy.class);

    /**
     * 包含一个上下文 每次调用getConnection 都会生成一个新的 代理对象也就是 对应一个专门的上下文对象
     */
    private ConnectionContext context = new ConnectionContext();

    private static final int DEFAULT_REPORT_RETRY_COUNT = 5;

    private static final int REPORT_RETRY_COUNT = ConfigurationFactory.getInstance().getInt(
        ConfigurationKeys.CLIENT_REPORT_RETRY_COUNT, DEFAULT_REPORT_RETRY_COUNT);

    private final static LockRetryPolicy LOCK_RETRY_POLICY = new LockRetryPolicy();

    /**
     * Instantiates a new Connection proxy.
     *
     * @param dataSourceProxy  the data source proxy
     * @param targetConnection the target connection
     */
    public ConnectionProxy(DataSourceProxy dataSourceProxy, Connection targetConnection) {
        super(dataSourceProxy, targetConnection);
    }

    /**
     * Gets context.
     *
     * @return the context
     */
    public ConnectionContext getContext() {
        return context;
    }

    /**
     * Bind.
     *
     * @param xid the xid
     */
    public void bind(String xid) {
        context.bind(xid);
    }

    /**
     * set global lock requires flag
     *
     * @param isLock whether to lock
     */
    public void setGlobalLockRequire(boolean isLock) {
        context.setGlobalLockRequire(isLock);
    }

    /**
     * get global lock requires flag
     */
    public boolean isGlobalLockRequire() {
        return context.isGlobalLockRequire();
    }

    /**
     * Check lock.
     * 检查当前锁状态
     * @param lockKeys the lockKeys
     * @throws SQLException the sql exception
     */
    public void checkLock(String lockKeys) throws SQLException {
        // Just check lock without requiring lock by now.
        try {
            // 判断是否可锁
            boolean lockable = DefaultResourceManager.get().lockQuery(BranchType.AT,
                getDataSourceProxy().getResourceId(), context.getXid(), lockKeys);
            if (!lockable) {
                throw new LockConflictException();
            }
        } catch (TransactionException e) {
            recognizeLockKeyConflictException(e, lockKeys);
        }
    }

    /**
     * Lock query.
     *
     * @param lockKeys the lock keys
     * @throws SQLException the sql exception
     */
    public boolean lockQuery(String lockKeys) throws SQLException {
        // Just check lock without requiring lock by now.
        boolean result = false;
        try {
            result = DefaultResourceManager.get().lockQuery(BranchType.AT, getDataSourceProxy().getResourceId(),
                context.getXid(), lockKeys);
        } catch (TransactionException e) {
            recognizeLockKeyConflictException(e, lockKeys);
        }
        return result;
    }

    /**
     * 判断传入的异常是否是 锁字段冲突
     * @param te
     * @throws SQLException
     */
    private void recognizeLockKeyConflictException(TransactionException te) throws SQLException {
        recognizeLockKeyConflictException(te, null);
    }

    /**
     * 判断传入的异常是否是 锁冲突异常
     * @param te
     * @param lockKeys
     * @throws SQLException
     */
    private void recognizeLockKeyConflictException(TransactionException te, String lockKeys) throws SQLException {
        if (te.getCode() == TransactionExceptionCode.LockKeyConflict) {
            StringBuilder reasonBuilder = new StringBuilder("get global lock fail, xid:" + context.getXid());
            if (StringUtils.isNotBlank(lockKeys)) {
                reasonBuilder.append(", lockKeys:" + lockKeys);
            }
            throw new LockConflictException(reasonBuilder.toString());
        } else {
            // 非冲突情况 抛出一个包装异常
            throw new SQLException(te);
        }

    }

    /**
     * append sqlUndoLog
     *
     * @param sqlUndoLog the sql undo log
     */
    public void appendUndoLog(SQLUndoLog sqlUndoLog) {
        context.appendUndoItem(sqlUndoLog);
    }

    /**
     * append lockKey
     *
     * @param lockKey the lock key
     */
    public void appendLockKey(String lockKey) {
        context.appendLockKey(lockKey);
    }

    /**
     * 执行 commit 方法 mybatis 中 实现各种加工逻辑 是通过装饰器模式 而这里 是代理模式
     * @throws SQLException
     */
    @Override
    public void commit() throws SQLException {
        try {
            LOCK_RETRY_POLICY.execute(() -> {
                // 传入一个 代表提交的call 对象
                doCommit();
                return null;
            });
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * Conn 的commit 方法被转发到这里
     * @throws SQLException
     */
    private void doCommit() throws SQLException {
        // 如果是全局事务  应该是连同下面所有的 branch 都提交
        if (context.inGlobalTransaction()) {
            // 处理全局事务提交
            processGlobalTransactionCommit();
            // 判断是否需要 获取全局锁
        } else if (context.isGlobalLockRequire()) {
            // 在获取全局锁的基础上 执行本地提交
            processLocalCommitWithGlobalLocks();
        } else {
            // 不需要就直接提交就好
            targetConnection.commit();
        }
    }

    /**
     * 在获取全局锁的基础上进行提交
     * @throws SQLException
     */
    private void processLocalCommitWithGlobalLocks() throws SQLException {

        // 构建锁语句 并执行
        checkLock(context.buildLockKeys());
        try {
            targetConnection.commit();
        } catch (Throwable ex) {
            throw new SQLException(ex);
        }
        // 已经提交的化 就可以重置掉上下文了
        context.reset();
    }

    /**
     * 代表在 分布式事务的环境下进行处理
     * @throws SQLException
     */
    private void processGlobalTransactionCommit() throws SQLException {
        try {
            // 将本次操作 作为全局事务中的 一个 branch 进行注册
            // 都要提交了 还注册什么???
            register();
        } catch (TransactionException e) {
            recognizeLockKeyConflictException(e, context.buildLockKeys());
        }

        try {
            // 将context 当前维护的所有 撤销日志 刷盘到某个地方
            if (context.hasUndoLog()) {
                UndoLogManagerFactory.getUndoLogManager(this.getDbType()).flushUndoLogs(this);
            }
            // 进行提交
            targetConnection.commit();
        } catch (Throwable ex) {
            LOGGER.error("process connectionProxy commit error: {}", ex.getMessage(), ex);
            report(false);
            throw new SQLException(ex);
        }
        report(true);
        context.reset();
    }

    /**
     * 将本事务进行注册
     * @throws TransactionException
     */
    private void register() throws TransactionException {
        Long branchId = DefaultResourceManager.get().branchRegister(BranchType.AT, getDataSourceProxy().getResourceId(),
            null, context.getXid(), null, context.buildLockKeys());
        context.setBranchId(branchId);
    }

    /**
     * 进行事务回滚
     * @throws SQLException
     */
    @Override
    public void rollback() throws SQLException {
        targetConnection.rollback();
        if (context.inGlobalTransaction()) {
            if (context.isBranchRegistered()) {
                report(false);
            }
        }
        context.reset();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if ((autoCommit) && !getAutoCommit()) {
            // change autocommit from false to true, we should commit() first according to JDBC spec.
            doCommit();
        }
        targetConnection.setAutoCommit(autoCommit);
    }

    private void report(boolean commitDone) throws SQLException {
        int retry = REPORT_RETRY_COUNT;
        while (retry > 0) {
            try {
                DefaultResourceManager.get().branchReport(BranchType.AT, context.getXid(), context.getBranchId(),
                    (commitDone ? BranchStatus.PhaseOne_Done : BranchStatus.PhaseOne_Failed), null);
                return;
            } catch (Throwable ex) {
                LOGGER.error("Failed to report [" + context.getBranchId() + "/" + context.getXid() + "] commit done ["
                    + commitDone + "] Retry Countdown: " + retry);
                retry--;

                if (retry == 0) {
                    throw new SQLException("Failed to report branch status " + commitDone, ex);
                }
            }
        }
    }

    /**
     * 锁重试策略
     */
    public static class LockRetryPolicy {
        protected final static boolean LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT =
                ConfigurationFactory.getInstance().getBoolean(ConfigurationKeys.CLIENT_LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT, true);

        /**
         * 如果上锁失败 选择重试 或者进行回滚
         * @param callable  call 对象内部封装了执行逻辑
         * @param <T>
         * @return
         * @throws Exception
         */
        public <T> T execute(Callable<T> callable) throws Exception {
            if (LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT) {
                return callable.call();
            } else {
                return doRetryOnLockConflict(callable);
            }
        }

        /**
         * 当上锁发生冲突时 进行重试
         * @param callable
         * @param <T>
         * @return
         * @throws Exception
         */
        protected <T> T doRetryOnLockConflict(Callable<T> callable) throws Exception {
            LockRetryController lockRetryController = new LockRetryController();
            while (true) {
                try {
                    return callable.call();
                    // 捕获冲突异常
                } catch (LockConflictException lockConflict) {
                    // 处理异常对象 由子类实现
                    onException(lockConflict);
                    lockRetryController.sleep(lockConflict);
                } catch (Exception e) {
                    onException(e);
                    throw e;
                }
            }
        }

        /**
         * Callback on exception in doLockRetryOnConflict.
         *
         * @param e invocation exception
         * @throws Exception error
         */
        protected void onException(Exception e) throws Exception {
        }
    }
}
