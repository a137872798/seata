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
     * 该对象内部可以维护一个 xid 代表该connection 正在处理哪个全局事务
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
            // 判断是否可锁  是通过与TC交互获取结果的
            boolean lockable = DefaultResourceManager.get().lockQuery(BranchType.AT,
                getDataSourceProxy().getResourceId(), context.getXid(), lockKeys);
            if (!lockable) {
                // 已上锁 抛出锁冲突异常
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
     * 本地事务的提交 还要上报到TC
     * @throws SQLException
     */
    @Override
    public void commit() throws SQLException {
        try {
            // 该对象在提交遇到异常时 会不断重试 直到成功
            LOCK_RETRY_POLICY.execute(() -> {
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
        // 如果是全局事务  将本结果上报到TC
        if (context.inGlobalTransaction()) {
            // 处理全局事务中提交
            processGlobalTransactionCommit();
            // 判断是否需要 获取全局锁
        } else if (context.isGlobalLockRequire()) {
            // TODO
            // 在获取全局锁的基础上 执行本地提交
            processLocalCommitWithGlobalLocks();
        } else {
            // 非全局事务相关注解修饰 正常处理
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
            // 将全局事务中的该分支 注册到TC 同时会将本事务中的主键上锁 配合 for update的查询 锁字段实现 读已提交
            register();
        } catch (TransactionException e) {
            // 判断是否是锁冲突异常   也就是这里实现了 不同的全局事务之间的写事务隔离  因为某个全局事务开始对某个字段进行修改时 其他事务是不能修改它的 但是允许读未提交
            // 一旦使用了 for update 实现类  也会变成 读已提交 也就是 必须确保字段还未加锁 有没有可能发生死锁呢???  虽然这里的重试是有次数的但是 会大幅度降低性能
            recognizeLockKeyConflictException(e, context.buildLockKeys());
        }

        try {
            // 在执行比如 insert delete 时 会通过 前后快照生成回滚日志 (具体生成规则没细看)
            if (context.hasUndoLog()) {
                // 实际上就是将 undo 日志保存到 本地数据库中
                UndoLogManagerFactory.getUndoLogManager(this.getDbType()).flushUndoLogs(this);
            }
            // 进行提交
            targetConnection.commit();
        } catch (Throwable ex) {
            LOGGER.error("process connectionProxy commit error: {}", ex.getMessage(), ex);
            // 代表提交过程出现异常 通知TC 进行回滚·
            report(false);
            throw new SQLException(ex);
        }
        // 提交成功情况下 通知 TC 完成了本地事务
        report(true);
        context.reset();
    }

    /**
     * 将本事务注册到TC 上  注意这里提交了 lockKeys 看来某个全局事务开始执行时关联到的所有主键就会被加锁 这样其他 全局事务尝试调用 for update 时就会发现
     * 相关主键被加锁 就不断自旋  实现 读已提交
     * @throws TransactionException
     */
    private void register() throws TransactionException {
        Long branchId = DefaultResourceManager.get().branchRegister(BranchType.AT, getDataSourceProxy().getResourceId(),
            null, context.getXid(), null, context.buildLockKeys());
        // 将返回的分事务id 设置到上下文中
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

    /**
     * 根据本地事务是否提交成功 将结果通知到TC
     * @param commitDone
     * @throws SQLException
     */
    private void report(boolean commitDone) throws SQLException {
        int retry = REPORT_RETRY_COUNT;
        while (retry > 0) {
            try {
                // 将本次结果提交到 TC 上 commitDone 为  true 代表一阶段成功 否则一阶段失败
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
                    // 捕获锁冲突异常  只有该异常会进行自旋重试
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
