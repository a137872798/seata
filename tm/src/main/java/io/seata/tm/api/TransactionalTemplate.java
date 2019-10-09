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
 * 全局事务模板   携带注解的 对应方法会被包装成该对象
 * @author sharajava
 */
public class TransactionalTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalTemplate.class);


    /**
     * Execute object.
     * 按照模板来执行事务
     * @param business the business  该对象内 包含了定义本次事务的参数 比如超时时间 事务名称
     * @return the object
     * @throws TransactionalExecutor.ExecutionException the execution exception
     */
    public Object execute(TransactionalExecutor business) throws Throwable {
        // 1. get or create a transaction
        // 如果本方法是第一个 就在当前线程中标记处在一个全局事务中  否则就加入到当前已有的全局事务
        GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();

        // 1.1 get transactionInfo
        // 获取注解信息
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
                // 执行业务逻辑  就是在这层会调用到其他需要事务的服务 这样 通过传播xid 其他服务也就包裹在一个事务中 (前提是其他服务方法被@GlobalTransactional 注解修饰)
                rs = business.execute();

            } catch (Throwable ex) {

                // 3.the needed business exception to rollback.
                // 当执行事务遇到异常时触发 这里的异常会抛到 execute() 之外
                // 如果内层的 事务 抛出异常了 会不断传递 直到 发起者
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
            // 清理用户设置的钩子
            cleanUp();
        }
    }

    /**
     * 遇到异常时 向TC 发起请求申请回滚全局事务
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
                // 回滚事务
                rollbackTransaction(tx, ex);
            } catch (TransactionException txe) {
                // Failed to rollback
                // 这里包装原始的异常后抛出
                throw new TransactionalExecutor.ExecutionException(tx, txe,
                        TransactionalExecutor.Code.RollbackFailure, ex);
            }
        } else {
            // not roll back on this exception, so commit
            // 如果该异常被配置不进行回滚 那么还是执行提交
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
     * 通知TC 回滚全局事务
     * @param tx
     * @param ex
     * @throws TransactionException
     * @throws TransactionalExecutor.ExecutionException
     */
    private void rollbackTransaction(GlobalTransaction tx, Throwable ex) throws TransactionException, TransactionalExecutor.ExecutionException {
        triggerBeforeRollback();
        // 核心方法
        tx.rollback();
        triggerAfterRollback();
        // 3.1 Successfully rolled back
        throw new TransactionalExecutor.ExecutionException(tx, TransactionalExecutor.Code.RollbackDone, ex);
    }

    /**
     * 预备开启一个全局事务
     * @param txInfo
     * @param tx
     * @throws TransactionalExecutor.ExecutionException
     */
    private void beginTransaction(TransactionInfo txInfo, GlobalTransaction tx) throws TransactionalExecutor.ExecutionException {
        try {
            // 获取所有前置钩子并执行
            triggerBeforeBegin();
            // 开始执行事务  实际上是 通过通知 TC 做协调
            tx.begin(txInfo.getTimeOut(), txInfo.getName());
            // 触发 begin 的后置钩子
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
