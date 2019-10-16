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
package io.seata.rm.datasource.exec;

import io.seata.rm.datasource.AbstractConnectionProxy;
import io.seata.rm.datasource.ConnectionProxy;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.sql.SQLRecognizer;
import io.seata.rm.datasource.sql.struct.TableRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

/**
 * The type Abstract dml base executor.
 * 基于dml 语句的执行器    dml 代表 insert/update/delete  不包含 select
 * @param <T> the type parameter
 * @param <S> the type parameter
 * @author sharajava
 */
public abstract class AbstractDMLBaseExecutor<T, S extends Statement> extends BaseTransactionalExecutor<T, S> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDMLBaseExecutor.class);

    /**
     * Instantiates a new Abstract dml base executor.
     *
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param sqlRecognizer     the sql recognizer
     */
    public AbstractDMLBaseExecutor(StatementProxy<S> statementProxy, StatementCallback<T, S> statementCallback,
                                   SQLRecognizer sqlRecognizer) {
        super(statementProxy, statementCallback, sqlRecognizer);
    }

    /**
     * @param args the args
     * @return
     * @throws Throwable
     */
    @Override
    public T doExecute(Object... args) throws Throwable {
        AbstractConnectionProxy connectionProxy = statementProxy.getConnectionProxy();
        // 如果开启自动提交 使用自动提交的方式进行执行  实际上这里应该都会关闭自动提交 (因为要将 原语句 和 生成的undo日志一起提交)
        if (connectionProxy.getAutoCommit()) {
            return executeAutoCommitTrue(args);
        } else {
            return executeAutoCommitFalse(args);
        }
    }

    /**
     * Execute auto commit false t.
     * 非自动提交情况下执行
     * @param args the args
     * @return the t
     * @throws Exception the exception
     */
    protected T executeAutoCommitFalse(Object[] args) throws Exception {
        // 生成快照 便于回滚
        TableRecords beforeImage = beforeImage();
        // 原生的jdbc执行
        T result = statementCallback.execute(statementProxy.getTargetStatement(), args);
        // 生成快照 便于回滚
        TableRecords afterImage = afterImage(beforeImage);
        // 生成undo 日志  并且将快照相关的数据 添加到上下文的锁字段中 因为该全局事务未完成 数据是不能被访问到的
        prepareUndoLog(beforeImage, afterImage);
        return result;
    }

    /**
     * Execute auto commit true t.
     * 以自动提交的方式执行
     * @param args the args
     * @return the t
     * @throws Throwable the throwable
     */
    protected T executeAutoCommitTrue(Object[] args) throws Throwable {
        AbstractConnectionProxy connectionProxy = statementProxy.getConnectionProxy();
        try {
            // 首先关闭自动提交 因为在开启全局事务的前提下 要将原语句与 undo 日志放到同一个本地事务中
            connectionProxy.setAutoCommit(false);
            // 构建重试对象并执行查询逻辑
            return new LockRetryPolicy(connectionProxy.getTargetConnection()).execute(() -> {
                // 复用非自动提交的逻辑
                T result = executeAutoCommitFalse(args);
                // connectionProxy 也对提交做了增强 每个本地事务的提交都需要将结果上报到TC 上
                connectionProxy.commit();
                return result;
            });
        } catch (Exception e) {
            // when exception occur in finally,this exception will lost, so just print it here
            LOGGER.error("execute executeAutoCommitTrue error:{}", e.getMessage(), e);
            if (!LockRetryPolicy.isLockRetryPolicyBranchRollbackOnConflict()) {
                connectionProxy.getTargetConnection().rollback();
            }
            throw e;
        } finally {
            // 执行完成后 将 标记在context 中的 全局事务标记 消除 并且恢复自动提交
            ((ConnectionProxy) connectionProxy).getContext().reset();
            connectionProxy.setAutoCommit(true);
        }
    }

    /**
     * Before image table records.
     *
     * @return the table records
     * @throws SQLException the sql exception
     */
    protected abstract TableRecords beforeImage() throws SQLException;

    /**
     * After image table records.
     * 生成after 对象  由子类实现
     * @param beforeImage the before image
     * @return the table records
     * @throws SQLException the sql exception
     */
    protected abstract TableRecords afterImage(TableRecords beforeImage) throws SQLException;

    /**
     * 上锁失败的 重试策略
     */
    private static class LockRetryPolicy extends ConnectionProxy.LockRetryPolicy {
        /**
         * 内部的连接对象
         */
        private final Connection connection;

        LockRetryPolicy(final Connection connection) {
            this.connection = connection;
        }

        /**
         * 执行逻辑 当失败时会触发重试
         * @param callable  call 对象内部封装了执行逻辑
         * @param <T>
         * @return
         * @throws Exception
         */
        @Override
        public <T> T execute(Callable<T> callable) throws Exception {
            // 当分布式事务中分事务尝试获取锁失败时 开启重试
            if (LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT) {
                return doRetryOnLockConflict(callable);
            } else {
                // 直接执行  如果获取全局锁失败会 抛出异常 而且不进行重试
                return callable.call();
            }
        }

        /**
         * 默认情况失败进行回滚
         * @param e invocation exception
         * @throws Exception
         */
        @Override
        protected void onException(Exception e) throws Exception {
            connection.rollback();
        }

        public static boolean isLockRetryPolicyBranchRollbackOnConflict() {
            return LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT;
        }
    }
}
