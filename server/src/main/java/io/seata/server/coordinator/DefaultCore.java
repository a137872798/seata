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
     * 获取到 RM 数据的输入流 一般就是TC 对象 TC 对象本身作为 server 接受RM 的数据 并转发给Core 进行处理
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
            // session 必须存活  关闭就对应着 commit 当全局事务已经提交后active 变成false 就不允许新的branch 提交了
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
            // 对branch 进行加锁  失败抛出异常  注册分事务之前应该还没有 执行本地任务 这时 要先对该数据加锁 避免其他事务尝试访问同一branch
            // 针对基于 DB 的实现 就是增加一条记录  注意 这里的 lockKey 不是必填的 也就是允许该branch 对其他事务读非提交 这样是为了保证性能 (需要使用者根据使用场景自行抉择)
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
            // branchId 使用UUID 生成
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
     * 收到 branch 的报告 (某个branch 的处理结果)
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
     * 查询能否上锁    锁的粒度是什么   lockKey 相关??? 还是连同xid resourceId 相关???
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
            // TCC 始终返回true  TODO 这里需要好好思考一下
            return true;
        }

    }

    /**
     * 开启一个全局事务   这里需要将全局事务持久化 否则一旦重启全局事务信息就会丢失
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
        // 创建一个全局事务对象
        GlobalSession session = GlobalSession.createGlobalSession(
            applicationId, transactionServiceGroup, name, timeout);
        // 设置 session 生命周期对应的监听器对象
        session.addSessionLifecycleListener(SessionHolder.getRootSessionManager());

        // 开启全局事务 这里会触发监听器 后面会委托给 TransactionStoreManager 将全局事务保存到数据库/file 中
        session.begin();

        //transaction start event
        // 在总线中开启一个 全局事务事件
        eventBus.post(new GlobalTransactionEvent(session.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
            session.getTransactionName(), session.getBeginTime(), null, session.getStatus()));

        LOGGER.info("Successfully begin global transaction xid = {}", session.getXid());
        return session.getXid();
    }

    /**
     * 提交全局事务
     * @param xid XID of the global transaction.
     * @return
     * @throws TransactionException
     */
    @Override
    public GlobalStatus commit(String xid) throws TransactionException {
        // 查找对应的 session  一种是通过 db 查询 一种通过存储在JVM的 map中
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid);
        if (globalSession == null) {
            return GlobalStatus.Finished;
        }
        globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
        // just lock changeStatus
        boolean shouldCommit = globalSession.lockAndExcute(() -> {
            //the lock should release after branch commit
            // close 代表设置 globalSession.active = false
            // clean 代表 释放分事务的锁  尽可能减少锁的范围吧 因为当确定要commit的时候 已经可以开始执行下个全局事务了(早在globalSession commit 之前 每个branch的本地事务都已经提交了 所以不存在
            // 可见性问题)
            globalSession
                .closeAndClean(); // Highlight: Firstly, close the session, then no more branch can be registered.
            // 全局事务必须处在 Begin 才允许提交 其他情况应该是代表 因为某个 branch 出现问题无法提交
            if (globalSession.getStatus() == GlobalStatus.Begin) {
                globalSession.changeStatus(GlobalStatus.Committing);
                return true;
            }
            return false;
        });
        // 如果无法提交 就返回当前全局事务的状态
        if (!shouldCommit) {
            return globalSession.getStatus();
        }
        // 判断同步提交还是异步提交
        if (globalSession.canBeCommittedAsync()) {
            // 异步提交 通过将任务添加到 asyncSessionManager中 配合一个定时器来执行定时任务 因为undo日志的删除(也就是branchcommit) 不是很重要所以允许异步化
            asyncCommit(globalSession);
            return GlobalStatus.Committed;
        } else {
            doGlobalCommit(globalSession, false);
        }
        return globalSession.getStatus();
    }

    /**
     * 同步提交  因为整个 globalTransaction 就是 由一个个branch组成的 而每个branch 都会完成自己的 本地事务 所以 commit 其实不需要做真正的提交
     * @param globalSession the global session
     * @param retrying      the retrying 代表是否正在重试中
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
                // 提交分事务  也就是正确的流程应该是
                //                                   begin GlobalTransaction -> do branchTransaction -> save undoLog -> commit branchTransaction
                //                                   -> commit GlobalTransaction -> delete undoLog(实际上下面的branchCommit就是这步 每个分事务会管理自己的本地事务)
                // 而在 RM 那端是使用一个 线程池来异步操作 即使本操作是异步执行对整个事务也不会有大影响无非就是一些无用的 undo日志 没有被删除
                // 当然上面的前提是针对 AT 情况 AT 和 TCC 是不同的
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
        // 提交完成后 变更事务状态为 Committed 并做持久化  并清除branchTransaction 的锁
        // end 会清除之前的 globalSession 持久化记录
        SessionHelper.endCommitted(globalSession);

        //committed event
        eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
            globalSession.getTransactionName(), globalSession.getBeginTime(), System.currentTimeMillis(),
            globalSession.getStatus()));

        LOGGER.info("Global[{}] committing is successfully done.", globalSession.getXid());

    }

    /**
     * 异步提交globalTransaction  就是指不删除 branchTransaction
     * @param globalSession
     * @throws TransactionException
     */
    private void asyncCommit(GlobalSession globalSession) throws TransactionException {
        globalSession.addSessionLifecycleListener(SessionHolder.getAsyncCommittingSessionManager());
        // 这里的 add 会变成update
        SessionHolder.getAsyncCommittingSessionManager().addGlobalSession(globalSession);
        // 这里会触发 监听器的 onStatusChange 钩子 (实际上是noop)
        globalSession.changeStatus(GlobalStatus.AsyncCommitting);
    }

    /**
     * 设置到 retryCommittionSessionManager 中
     * @param globalSession
     * @throws TransactionException
     */
    private void queueToRetryCommit(GlobalSession globalSession) throws TransactionException {
        globalSession.addSessionLifecycleListener(SessionHolder.getRetryCommittingSessionManager());
        // 这里是 update
        SessionHolder.getRetryCommittingSessionManager().addGlobalSession(globalSession);
        // 这里是 noop
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
     * 回滚全局事务
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
        // 判断是否需要回滚  该锁是一个JVM 锁 那么如何确保某个全局事务总是能访问到 某个单点TC (每次总是与该地址进行通信)
        boolean shouldRollBack = globalSession.lockAndExcute(() -> {
            // 首先关闭全局事务 避免更多的 branch 注册上来
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

    /**
     * 执行回滚操作
     * @param globalSession the global session
     * @param retrying      the retrying 代表是否是在重试定时器中执行的
     * @throws TransactionException
     */
    @Override
    public void doGlobalRollback(GlobalSession globalSession, boolean retrying) throws TransactionException {
        //start rollback event  往事件总线中插入 回滚任务  推测 该对象不影响主流程 而是类似于一种增强 用户通过为 总线对象设置订阅者 补充自己的逻辑
        eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
            globalSession.getTransactionName(), globalSession.getBeginTime(), null, globalSession.getStatus()));

        // 将 总事务下 所有分事务 倒序返回
        for (BranchSession branchSession : globalSession.getReverseSortedBranches()) {
            BranchStatus currentBranchStatus = branchSession.getStatus();
            // 一阶段失败 不需要回滚 代表 某个本地事务 第一次提交就失败了  提交方式为 除了提交正常的数据外 还会提交一个 undo 日志
            if (currentBranchStatus == BranchStatus.PhaseOne_Failed) {
                globalSession.removeBranch(branchSession);
                continue;
            }
            try {
                // 因为TC 本身只是用于发起回滚请求 实际的操作还是需要通过server 通知RM 进行处理
                // 这里的回滚就会利用之前保存的 undo 日志
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
                        // 本身是非重试的情况才允许加入
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
        // 执行回滚后的清理工作
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
