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
import io.seata.core.exception.TransactionException;
import io.seata.tm.api.transaction.TransactionHook;
import io.seata.tm.api.transaction.TransactionHookManager;
import io.seata.tm.api.transaction.TransactionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Template of executing business logic with a global transaction.
 *
 * @author sharajava
 */
public class TransactionalTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalTemplate.class);


    /**
     * Execute object.
     * 按照模板来执行事务
     * @param business the business
     * @return the object
     * @throws TransactionalExecutor.ExecutionException the execution exception
     */
    public Object execute(TransactionalExecutor business) throws Throwable {
        // 1. get or create a transaction
        // 创建全局事务
        GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();

        // 1.1 get transactionInfo
        // 获取全局事务信息
        TransactionInfo txInfo = business.getTransactionInfo();
        if (txInfo == null) {
            throw new ShouldNeverHappenException("transactionInfo does not exist");
        }
        try {

            // 2. begin transaction
            // 开始执行事务
            beginTransaction(txInfo, tx);

            Object rs = null;
            try {

                // Do Your Business
                // 执行业务逻辑
                rs = business.execute();

            } catch (Throwable ex) {

                // 3.the needed business exception to rollback.
                // 当执行事务遇到异常时触发
                completeTransactionAfterThrowing(txInfo,tx,ex);
                throw ex;
            }

            // 4. everything is fine, commit.
            // 提交事务
            commitTransaction(tx);

            return rs;
        } finally {
            //5. clear
            // 触发结束任务
            triggerAfterCompletion();
            // 做清理工作
            cleanUp();
        }
    }

    /**
     * 遇到异常时
     * @param txInfo
     * @param tx
     * @param ex
     * @throws TransactionalExecutor.ExecutionException
     */
    private void completeTransactionAfterThrowing(TransactionInfo txInfo, GlobalTransaction tx, Throwable ex) throws TransactionalExecutor.ExecutionException {
        //roll back
        // 代表遇到该异常允许进行回滚
        if (txInfo != null && txInfo.rollbackOn(ex)) {
            try {
                rollbackTransaction(tx, ex);
            } catch (TransactionException txe) {
                // Failed to rollback
                throw new TransactionalExecutor.ExecutionException(tx, txe,
                        TransactionalExecutor.Code.RollbackFailure, ex);
            }
        } else {
            // not roll back on this exception, so commit
            // 不处理还是选择 提交
            commitTransaction(tx);
        }
    }

    /**
     * 提交事务
     * @param tx
     * @throws TransactionalExecutor.ExecutionException
     */
    private void commitTransaction(GlobalTransaction tx) throws TransactionalExecutor.ExecutionException {
        try {
            // 提交前钩子
            triggerBeforeCommit();
            tx.commit();
            // 提交后钩子
            triggerAfterCommit();
        } catch (TransactionException txe) {
            // 4.1 Failed to commit
            throw new TransactionalExecutor.ExecutionException(tx, txe,
                TransactionalExecutor.Code.CommitFailure);
        }
    }

    /**
     * 回滚关联的方法
     * @param tx
     * @param ex
     * @throws TransactionException
     * @throws TransactionalExecutor.ExecutionException
     */
    private void rollbackTransaction(GlobalTransaction tx, Throwable ex) throws TransactionException, TransactionalExecutor.ExecutionException {
        triggerBeforeRollback();
        tx.rollback();
        triggerAfterRollback();
        // 3.1 Successfully rolled back
        throw new TransactionalExecutor.ExecutionException(tx, TransactionalExecutor.Code.RollbackDone, ex);
    }

    private void beginTransaction(TransactionInfo txInfo, GlobalTransaction tx) throws TransactionalExecutor.ExecutionException {
        try {
            triggerBeforeBegin();
            tx.begin(txInfo.getTimeOut(), txInfo.getName());
            triggerAfterBegin();
        } catch (TransactionException txe) {
            throw new TransactionalExecutor.ExecutionException(tx, txe,
                TransactionalExecutor.Code.BeginFailure);

        }
    }

    private void triggerBeforeBegin() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.beforeBegin();
            } catch (Exception e) {
                LOGGER.error("Failed execute beforeBegin in hook " + e.getMessage());
            }
        }
    }

    private void triggerAfterBegin() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.afterBegin();
            } catch (Exception e) {
                LOGGER.error("Failed execute afterBegin in hook " + e.getMessage());
            }
        }
    }

    private void triggerBeforeRollback() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.beforeRollback();
            } catch (Exception e) {
                LOGGER.error("Failed execute beforeRollback in hook " + e.getMessage());
            }
        }
    }

    private void triggerAfterRollback() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.afterRollback();
            } catch (Exception e) {
                LOGGER.error("Failed execute afterRollback in hook " + e.getMessage());
            }
        }
    }

    private void triggerBeforeCommit() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.beforeCommit();
            } catch (Exception e) {
                LOGGER.error("Failed execute beforeCommit in hook " + e.getMessage());
            }
        }
    }

    private void triggerAfterCommit() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.afterCommit();
            } catch (Exception e) {
                LOGGER.error("Failed execute afterCommit in hook " + e.getMessage());
            }
        }
    }

    private void triggerAfterCompletion() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.afterCompletion();
            } catch (Exception e) {
                LOGGER.error("Failed execute afterCompletion in hook " + e.getMessage());
            }
        }
    }

    private void cleanUp() {
        TransactionHookManager.clear();
    }

    private List<TransactionHook> getCurrentHooks() {
        return TransactionHookManager.getHooks();
    }

}
