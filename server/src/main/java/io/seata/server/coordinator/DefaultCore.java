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
package io.seata.server.coordinator;

import io.seata.core.event.EventBus;
import io.seata.core.event.GlobalTransactionEvent;
import io.seata.core.exception.BranchTransactionException;
import io.seata.core.exception.GlobalTransactionException;
import io.seata.core.exception.TransactionException;
import io.seata.core.exception.TransactionExceptionCode;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.BranchType;
import io.seata.core.model.GlobalStatus;
import io.seata.core.model.ResourceManagerInbound;
import io.seata.server.event.EventBusManager;
import io.seata.server.lock.LockManager;
import io.seata.server.lock.LockerFactory;
import io.seata.server.session.BranchSession;
import io.seata.server.session.GlobalSession;
import io.seata.server.session.SessionHelper;
import io.seata.server.session.SessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.seata.core.exception.TransactionExceptionCode.BranchTransactionNotExist;
import static io.seata.core.exception.TransactionExceptionCode.FailedToAddBranch;
import static io.seata.core.exception.TransactionExceptionCode.GlobalTransactionNotActive;
import static io.seata.core.exception.TransactionExceptionCode.GlobalTransactionStatusInvalid;
import static io.seata.core.exception.TransactionExceptionCode.LockKeyConflict;

/**
 * The type Default core.
 * 默认的核心对象
 * @author sharajava
 */
public class DefaultCore implements Core {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCore.class);

    /**
     * 跟锁相关的管理对象
     */
    private LockManager lockManager = LockerFactory.getLockManager();

    /**
     * 处理 RM输入数据
     */
    private ResourceManagerInbound resourceManagerInbound;

    /**
     * 事件总线  要将关注的事件先注册到bus 上
     */
    private EventBus eventBus = EventBusManager.get();

    @Override
    public void setResourceManagerInbound(ResourceManagerInbound resourceManagerInbound) {
        this.resourceManagerInbound = resourceManagerInbound;
    }

    /**
     * 处理分事务的注册 这里是注册到globalSession 上
     * @param branchType the branch type  是 AT 类型 还是 TCC 类型
     * @param resourceId the resource id   分事务唯一id
     * @param clientId   the client id     对应的客户端id
     * @param xid        the xid           总事务id
     * @param applicationData the context    数据信息
     * @param lockKeys   the lock keys    需要上锁的字段
     * @return
     * @throws TransactionException
     */
    @Override
    public Long branchRegister(BranchType branchType, String resourceId, String clientId, String xid,
                               String applicationData, String lockKeys) throws TransactionException {
        // 首先确保存在全局session
        GlobalSession globalSession = assertGlobalSessionNotNull(xid);
        // 加锁并执行任务
        return globalSession.lockAndExcute(() -> {
            // session 必须存活
            if (!globalSession.isActive()) {
                throw new GlobalTransactionException(GlobalTransactionNotActive,
                    String.format("Could not register branch into global session xid = %s status = %s", globalSession.getXid(), globalSession.getStatus()));
            }
            // 确保状态是 begin
            if (globalSession.getStatus() != GlobalStatus.Begin) {
                throw new GlobalTransactionException(GlobalTransactionStatusInvalid,
                    String.format("Could not register branch into global session xid = %s status = %s while expecting %s", globalSession.getXid(), globalSession.getStatus(), GlobalStatus.Begin));
            }
            // 设置管理器对象
            globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
            // 根据信息生成一个 branchSession 对象
            BranchSession branchSession = SessionHelper.newBranchByGlobal(globalSession, branchType, resourceId,
                applicationData, lockKeys, clientId);
            // 对branch 进行加锁  失败抛出异常
            if (!branchSession.lock()) {
                throw new BranchTransactionException(LockKeyConflict,
                    String.format("Global lock acquire failed xid = %s branchId = %s", globalSession.getXid(), branchSession.getBranchId()));
            }
            try {
                // 为全局session 增加 branch session
                globalSession.addBranch(branchSession);
            } catch (RuntimeException ex) {
                branchSession.unlock();
                throw new BranchTransactionException(FailedToAddBranch,
                    String.format("Failed to store branch xid = %s branchId = %s", globalSession.getXid(), branchSession.getBranchId()));
            }
            LOGGER.info("Successfully register branch xid = {}, branchId = {}", globalSession.getXid(), branchSession.getBranchId());
            return branchSession.getBranchId();
        });
    }

    private GlobalSession assertGlobalSessionNotNull(String xid) throws TransactionException {
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid);
        if (globalSession == null) {
            throw new GlobalTransactionException(TransactionExceptionCode.GlobalTransactionNotExist, String.format("Could not found global transaction xid = %s", xid));
        }
        return globalSession;
    }

    /**
     * 报告
     * @param branchType      the branch type
     * @param xid             the xid
     * @param branchId        the branch id
     * @param status          the status
     * @param applicationData the application data
     * @throws TransactionException
     */
    @Override
    public void branchReport(BranchType branchType, String xid, long branchId, BranchStatus status,
                             String applicationData) throws TransactionException {
        GlobalSession globalSession = assertGlobalSessionNotNull(xid);
        BranchSession branchSession = globalSession.getBranch(branchId);
        if (branchSession == null) {
            throw new BranchTransactionException(BranchTransactionNotExist, String.format("Could not found branch session xid = %s branchId = %s", xid, branchId));
        }
        globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
        // 将某个分事务的状态改变
        globalSession.changeBranchStatus(branchSession, status);

        LOGGER.info("Successfully branch report xid = {}, branchId = {}", globalSession.getXid(), branchSession.getBranchId());
    }

    /**
     * 查询能否上锁
     * @param branchType the branch type
     * @param resourceId the resource id
     * @param xid        the xid
     * @param lockKeys   the lock keys
     * @return
     * @throws TransactionException
     */
    @Override
    public boolean lockQuery(BranchType branchType, String resourceId, String xid, String lockKeys)
        throws TransactionException {
        if (branchType == BranchType.AT) {
            return lockManager.isLockable(xid, resourceId, lockKeys);
        } else {
            // TCC 始终返回true
            return true;
        }

    }

    /**
     * 开启一个全局事务
     * @param applicationId           ID of the application who begins this transaction.
     * @param transactionServiceGroup ID of the transaction service group.
     * @param name                    Give a name to the global transaction.
     * @param timeout                 Timeout of the global transaction.
     * @return
     * @throws TransactionException
     */
    @Override
    public String begin(String applicationId, String transactionServiceGroup, String name, int timeout)
        throws TransactionException {
        GlobalSession session = GlobalSession.createGlobalSession(
            applicationId, transactionServiceGroup, name, timeout);
        session.addSessionLifecycleListener(SessionHolder.getRootSessionManager());

        // 开启全局事务 这里只会触发监听器
        session.begin();

        //transaction start event
        // 在总线中开启一个 全局事务事件
        eventBus.post(new GlobalTransactionEvent(session.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
            session.getTransactionName(), session.getBeginTime(), null, session.getStatus()));

        LOGGER.info("Successfully begin global transaction xid = {}", session.getXid());
        return session.getXid();
    }

    /**
     * 代表某个全局事务完成 并进行了提交
     * @param xid XID of the global transaction.
     * @return
     * @throws TransactionException
     */
    @Override
    public GlobalStatus commit(String xid) throws TransactionException {
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid);
        if (globalSession == null) {
            return GlobalStatus.Finished;
        }
        globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
        // just lock changeStatus
        boolean shouldCommit = globalSession.lockAndExcute(() -> {
            //the lock should release after branch commit
            // 关闭session 就是释放session所持有的锁
            globalSession
                .closeAndClean(); // Highlight: Firstly, close the session, then no more branch can be registered.
            // 代表正在提交中
            if (globalSession.getStatus() == GlobalStatus.Begin) {
                globalSession.changeStatus(GlobalStatus.Committing);
                return true;
            }
            return false;
        });
        if (!shouldCommit) {
            return globalSession.getStatus();
        }
        // 判断同步提交还是异步提交
        if (globalSession.canBeCommittedAsync()) {
            asyncCommit(globalSession);
            return GlobalStatus.Committed;
        } else {
            doGlobalCommit(globalSession, false);
        }
        return globalSession.getStatus();
    }

    /**
     * 同步提交  该方法中没有看到实际的 commit 动作
     * @param globalSession the global session
     * @param retrying      the retrying
     * @throws TransactionException
     */
    @Override
    public void doGlobalCommit(GlobalSession globalSession, boolean retrying) throws TransactionException {
        //start committing event
        eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
            globalSession.getTransactionName(), globalSession.getBeginTime(), null, globalSession.getStatus()));

        for (BranchSession branchSession : globalSession.getSortedBranches()) {
            BranchStatus currentStatus = branchSession.getStatus();
            // 如果一阶段提交失败
            if (currentStatus == BranchStatus.PhaseOne_Failed) {
                // 移除分事务
                globalSession.removeBranch(branchSession);
                continue;
            }
            try {
                // 处理分事务的提交
                BranchStatus branchStatus = resourceManagerInbound.branchCommit(branchSession.getBranchType(),
                    branchSession.getXid(), branchSession.getBranchId(),
                    branchSession.getResourceId(), branchSession.getApplicationData());

                switch (branchStatus) {
                    // 已提交情况就移除该session
                    case PhaseTwo_Committed:
                        globalSession.removeBranch(branchSession);
                        continue;
                    // 二阶段事务提交失败且不允许重试
                    case PhaseTwo_CommitFailed_Unretryable:
                        if (globalSession.canBeCommittedAsync()) {
                            LOGGER.error("By [{}], failed to commit branch {}", branchStatus, branchSession);
                            continue;
                        } else {
                            // 当提交失败时触发 就是修改status
                            SessionHelper.endCommitFailed(globalSession);
                            LOGGER.error("Finally, failed to commit global[{}] since branch[{}] commit failed",
                                globalSession.getXid(), branchSession.getBranchId());
                            return;
                        }
                    default:
                        // 设置成重试中  不是不允许重试吗???
                        if (!retrying) {
                            queueToRetryCommit(globalSession);
                            return;
                        }
                        if (globalSession.canBeCommittedAsync()) {
                            LOGGER.error("By [{}], failed to commit branch {}", branchStatus, branchSession);
                            continue;
                        } else {
                            LOGGER.error(
                                "Failed to commit global[{}] since branch[{}] commit failed, will retry later.",
                                globalSession.getXid(), branchSession.getBranchId());
                            return;
                        }

                }

            } catch (Exception ex) {
                LOGGER.error("Exception committing branch {}", branchSession, ex);
                if (!retrying) {
                    queueToRetryCommit(globalSession);
                    throw new TransactionException(ex);
                }

            }

        }
        if (globalSession.hasBranch()) {
            LOGGER.info("Global[{}] committing is NOT done.", globalSession.getXid());
            return;
        }
        SessionHelper.endCommitted(globalSession);

        //committed event
        eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
            globalSession.getTransactionName(), globalSession.getBeginTime(), System.currentTimeMillis(),
            globalSession.getStatus()));

        LOGGER.info("Global[{}] committing is successfully done.", globalSession.getXid());

    }

    /**
     * 异步提交
     * @param globalSession
     * @throws TransactionException
     */
    private void asyncCommit(GlobalSession globalSession) throws TransactionException {
        globalSession.addSessionLifecycleListener(SessionHolder.getAsyncCommittingSessionManager());
        // 将globalSession 设置到了 ACSM 中
        SessionHolder.getAsyncCommittingSessionManager().addGlobalSession(globalSession);
        globalSession.changeStatus(GlobalStatus.AsyncCommitting);
    }

    /**
     * 设置到 retryCommittionSessionManager 中
     * @param globalSession
     * @throws TransactionException
     */
    private void queueToRetryCommit(GlobalSession globalSession) throws TransactionException {
        globalSession.addSessionLifecycleListener(SessionHolder.getRetryCommittingSessionManager());
        SessionHolder.getRetryCommittingSessionManager().addGlobalSession(globalSession);
        globalSession.changeStatus(GlobalStatus.CommitRetrying);
    }

    /**
     * 设置到回滚 SM 中
     * @param globalSession
     * @throws TransactionException
     */
    private void queueToRetryRollback(GlobalSession globalSession) throws TransactionException {
        globalSession.addSessionLifecycleListener(SessionHolder.getRetryRollbackingSessionManager());
        SessionHolder.getRetryRollbackingSessionManager().addGlobalSession(globalSession);
        GlobalStatus currentStatus = globalSession.getStatus();
        if (SessionHelper.isTimeoutGlobalStatus(currentStatus)) {
            globalSession.changeStatus(GlobalStatus.TimeoutRollbackRetrying);
        } else {
            globalSession.changeStatus(GlobalStatus.RollbackRetrying);
        }
    }

    /**
     * 执行回滚逻辑
     * @param xid XID of the global transaction
     * @return
     * @throws TransactionException
     */
    @Override
    public GlobalStatus rollback(String xid) throws TransactionException {
        // 从map 中根据xid 查询globalSession
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid);
        if (globalSession == null) {
            return GlobalStatus.Finished;
        }
        globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
        // just lock changeStatus
        // 判断是否需要回滚
        boolean shouldRollBack = globalSession.lockAndExcute(() -> {
            globalSession.close(); // Highlight: Firstly, close the session, then no more branch can be registered.
            // 只有globalSession 处于开始阶段才允许回滚
            if (globalSession.getStatus() == GlobalStatus.Begin) {
                globalSession.changeStatus(GlobalStatus.Rollbacking);
                return true;
            }
            return false;
        });
        if (!shouldRollBack) {
            return globalSession.getStatus();
        }

        // 执行回滚操作
        doGlobalRollback(globalSession, false);
        return globalSession.getStatus();
    }

    @Override
    public void doGlobalRollback(GlobalSession globalSession, boolean retrying) throws TransactionException {
        //start rollback event
        eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
            globalSession.getTransactionName(), globalSession.getBeginTime(), null, globalSession.getStatus()));

        for (BranchSession branchSession : globalSession.getReverseSortedBranches()) {
            BranchStatus currentBranchStatus = branchSession.getStatus();
            // 一阶段失败 就移除
            if (currentBranchStatus == BranchStatus.PhaseOne_Failed) {
                globalSession.removeBranch(branchSession);
                continue;
            }
            try {
                // 处理传入的 回滚请求  这里代表真正执行回滚逻辑
                BranchStatus branchStatus = resourceManagerInbound.branchRollback(branchSession.getBranchType(),
                    branchSession.getXid(), branchSession.getBranchId(),
                    branchSession.getResourceId(), branchSession.getApplicationData());

                switch (branchStatus) {
                    // 二阶段回滚失败 进行移除
                    case PhaseTwo_Rollbacked:
                        globalSession.removeBranch(branchSession);
                        LOGGER.info("Successfully rollback branch xid={} branchId={}", globalSession.getXid(), branchSession.getBranchId());
                        continue;
                        // 代表不允许重试 触发 rollbackFailed
                    case PhaseTwo_RollbackFailed_Unretryable:
                        SessionHelper.endRollbackFailed(globalSession);
                        LOGGER.info("Failed to rollback branch and stop retry xid={} branchId={}", globalSession.getXid(), branchSession.getBranchId());
                        return;
                        // 进入重试队列
                    default:
                        LOGGER.info("Failed to rollback branch xid={} branchId={}", globalSession.getXid(), branchSession.getBranchId());
                        if (!retrying) {
                            queueToRetryRollback(globalSession);
                        }
                        return;

                }
            } catch (Exception ex) {
                LOGGER.error("Exception rollbacking branch xid={} branchId={}", globalSession.getXid(), branchSession.getBranchId(), ex);
                if (!retrying) {
                    queueToRetryRollback(globalSession);
                }
                throw new TransactionException(ex);
            }

        }
        SessionHelper.endRollbacked(globalSession);

        //rollbacked event
        eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
            globalSession.getTransactionName(), globalSession.getBeginTime(), System.currentTimeMillis(),
            globalSession.getStatus()));

        LOGGER.info("Successfully rollback global, xid = {}", globalSession.getXid());
    }

    @Override
    public GlobalStatus getStatus(String xid) throws TransactionException {
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid);
        if (null == globalSession) {
            return GlobalStatus.Finished;
        } else {
            return globalSession.getStatus();
        }
    }
}
