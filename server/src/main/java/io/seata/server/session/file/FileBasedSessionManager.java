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
package io.seata.server.session.file;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.common.loader.LoadLevel;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.model.GlobalStatus;
import io.seata.core.store.StoreMode;
import io.seata.server.UUIDGenerator;
import io.seata.server.session.BranchSession;
import io.seata.server.session.DefaultSessionManager;
import io.seata.server.session.GlobalSession;
import io.seata.server.session.Reloadable;
import io.seata.server.session.SessionManager;
import io.seata.server.store.ReloadableStore;
import io.seata.server.store.SessionStorable;
import io.seata.server.store.TransactionStoreManager;
import io.seata.server.store.TransactionWriteStore;

/**
 * The type File based session manager.
 * 基于文件系统的   注意该对象实现了 reloadable 接口
 * @author jimin.jm @alibaba-inc.com
 */
@LoadLevel(name = "file")
public class FileBasedSessionManager extends DefaultSessionManager implements Reloadable {

    private static final int READ_SIZE = ConfigurationFactory.getInstance().getInt(
        ConfigurationKeys.SERVICE_SESSION_RELOAD_READ_SIZE, 100);

    /**
     * Instantiates a new File based session manager.
     * 初始化基于文件的存储
     * @param name                 the name               文件名
     * @param sessionStoreFilePath the session store file path     存储路径
     * @throws IOException the io exception
     */
    public FileBasedSessionManager(String name, String sessionStoreFilePath) throws IOException {
        super(name);
        // 创建基于文件的 事务存储对象
        transactionStoreManager = EnhancedServiceLoader.load(TransactionStoreManager.class, StoreMode.FILE.name(),
            new Class[] {String.class, SessionManager.class},
            new Object[] {sessionStoreFilePath + File.separator + name, this});
    }

    /**
     * 一般是当 server重启时 重新加载数据
     */
    @Override
    public void reload() {
        // 从文件中重新读取 会话数据 并保存到 二级缓存中
        restoreSessions();
        // 清除掉已经完成的 session 对象 (因为从文件中加载的应该是全部数据)
        washSessions();
    }

    /**
     * 重新加载session
     */
    private void restoreSessions() {
        Map<Long, BranchSession> unhandledBranchBuffer = new HashMap<>();

        // isHistory 分别以 true 和 false 执行一次  从 TSM 中找到 session 对象并更新本对象内置map (本地二级缓存)的 数据  unhandledBranchBuffer 内存放的是没有找到的分事务
        restoreSessions(true, unhandledBranchBuffer);
        restoreSessions(false, unhandledBranchBuffer);

        // 代表发现了 某些 branchSession没有对应的globalSession 对象
        if (!unhandledBranchBuffer.isEmpty()) {
            unhandledBranchBuffer.values().forEach(branchSession -> {
                String xid = branchSession.getXid();
                long bid = branchSession.getBranchId();
                GlobalSession found = sessionMap.get(xid);
                // 这里只是做打印日志没有特别处理
                if (found == null) {
                    // Ignore
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("GlobalSession Does Not Exists For BranchSession [" + bid + "/" + xid + "]");
                    }
                } else {
                    BranchSession existingBranch = found.getBranch(branchSession.getBranchId());
                    if (existingBranch == null) {
                        found.add(branchSession);
                    } else {
                        existingBranch.setStatus(branchSession.getStatus());
                    }
                }

            });
        }
    }

    /**
     * 清除已经处理完的 session
     */
    private void washSessions() {
        // 存在 session 的情况下
        if (sessionMap.size() > 0) {
            Iterator<Map.Entry<String, GlobalSession>> iterator = sessionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                GlobalSession globalSession = iterator.next().getValue();

                GlobalStatus globalStatus = globalSession.getStatus();
                switch (globalStatus) {
                    case UnKnown:
                    case Committed:
                    case CommitFailed:
                    case Rollbacked:
                    case RollbackFailed:
                    case TimeoutRollbacked:
                    case TimeoutRollbackFailed:
                        // 移除已处理完成的 globalSession
                    case Finished:
                        // Remove all sessions finished
                        iterator.remove();
                        break;
                    default:
                        break;
                }
            }
        }
    }

    /**
     * 重新加载 数据 首先确保 TSM 实现了 reloadable 接口
     * @param isHistory
     * @param unhandledBranchBuffer
     */
    private void restoreSessions(boolean isHistory, Map<Long, BranchSession> unhandledBranchBuffer) {
        if (!(transactionStoreManager instanceof ReloadableStore)) {
            return;
        }
        // 还有数据的情况就不断的 读取  根据 isHistory 判断读取 历史文件还是 正常文件
        while (((ReloadableStore)transactionStoreManager).hasRemaining(isHistory)) {
            List<TransactionWriteStore> stores = ((ReloadableStore)transactionStoreManager).readWriteStore(READ_SIZE,
                isHistory);
            // 将读取出来的数据 保存到 stores 中
            restore(stores, unhandledBranchBuffer);
        }
    }

    /**
     * 将TrasncationWriteStore 转换成 BranchSession 并保存到对应容器中  （TransactionWriteStore 可能是全局事务 也可能是分支事务）
     * @param stores
     * @param unhandledBranchSessions   当该分支事务所属的global事务不存在时 加入到map中
     */
    private void restore(List<TransactionWriteStore> stores, Map<Long, BranchSession> unhandledBranchSessions) {
        long maxRecoverId = UUIDGenerator.getCurrentUUID();
        for (TransactionWriteStore store : stores) {
            // 获取写入对象 对应的 操作
            TransactionStoreManager.LogOperation logOperation = store.getOperate();
            // 获取当时写入使用的 请求对象
            SessionStorable sessionStorable = store.getSessionRequest();
            // 获取更大的id
            maxRecoverId = getMaxId(maxRecoverId, sessionStorable);
            switch (logOperation) {
                // 如果是针对全局事务的增加或者更新
                case GLOBAL_ADD:
                case GLOBAL_UPDATE: {
                    GlobalSession globalSession = (GlobalSession)sessionStorable;
                    if (globalSession.getTransactionId() == 0) {
                        LOGGER.error(
                            "Restore globalSession from file failed, the transactionId is zero , xid:" + globalSession
                                .getXid());
                        break;
                    }
                    // 找到对应的globalsession
                    GlobalSession foundGlobalSession = sessionMap.get(globalSession.getXid());
                    // 代表是 这里先加入到内存中 是作为一种二级缓存吧 避免 每次必须通过文件访问数据 推测有个异步线程将内存中的数据写入到 文件中
                    if (foundGlobalSession == null) {
                        sessionMap.put(globalSession.getXid(), globalSession);
                    } else {
                        // 代表更新
                        foundGlobalSession.setStatus(globalSession.getStatus());
                    }
                    break;
                }
                case GLOBAL_REMOVE: {
                    GlobalSession globalSession = (GlobalSession)sessionStorable;
                    if (globalSession.getTransactionId() == 0) {
                        LOGGER.error(
                            "Restore globalSession from file failed, the transactionId is zero , xid:" + globalSession
                                .getXid());
                        break;
                    }
                    if (sessionMap.remove(globalSession.getXid()) == null) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("GlobalSession To Be Removed Does Not Exists [" + globalSession.getXid() + "]");
                        }
                    }
                    break;
                }
                case BRANCH_ADD:
                case BRANCH_UPDATE: {
                    BranchSession branchSession = (BranchSession)sessionStorable;
                    if (branchSession.getTransactionId() == 0) {
                        LOGGER.error(
                            "Restore branchSession from file failed, the transactionId is zero , xid:" + branchSession
                                .getXid());
                        break;
                    }
                    GlobalSession foundGlobalSession = sessionMap.get(branchSession.getXid());
                    if (foundGlobalSession == null) {
                        unhandledBranchSessions.put(branchSession.getBranchId(), branchSession);
                    } else {
                        BranchSession existingBranch = foundGlobalSession.getBranch(branchSession.getBranchId());
                        if (existingBranch == null) {
                            foundGlobalSession.add(branchSession);
                        } else {
                            existingBranch.setStatus(branchSession.getStatus());
                        }
                    }
                    break;
                }
                case BRANCH_REMOVE: {
                    BranchSession branchSession = (BranchSession)sessionStorable;
                    String xid = branchSession.getXid();
                    long bid = branchSession.getBranchId();
                    if (branchSession.getTransactionId() == 0) {
                        LOGGER.error(
                            "Restore branchSession from file failed, the transactionId is zero , xid:" + branchSession
                                .getXid());
                        break;
                    }
                    GlobalSession found = sessionMap.get(xid);
                    if (found == null) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info(
                                "GlobalSession To Be Updated (Remove Branch) Does Not Exists [" + bid + "/" + xid
                                    + "]");
                        }
                    } else {
                        BranchSession theBranch = found.getBranch(bid);
                        if (theBranch == null) {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("BranchSession To Be Updated Does Not Exists [" + bid + "/" + xid + "]");
                            }
                        } else {
                            found.remove(theBranch);
                        }
                    }
                    break;
                }

                default:
                    throw new ShouldNeverHappenException("Unknown Operation: " + logOperation);

            }
        }
        setMaxId(maxRecoverId);

    }

    /**
     * 获取更大的id
     * @param maxRecoverId
     * @param sessionStorable
     * @return
     */
    private long getMaxId(long maxRecoverId, SessionStorable sessionStorable) {
        long currentId = 0;
        if (sessionStorable instanceof GlobalSession) {
            currentId = ((GlobalSession)sessionStorable).getTransactionId();
        } else if (sessionStorable instanceof BranchSession) {
            currentId = ((BranchSession)sessionStorable).getBranchId();
        }

        return maxRecoverId > currentId ? maxRecoverId : currentId;
    }

    private void setMaxId(long maxRecoverId) {
        long currentId;
        // will be recover multi-thread later
        while ((currentId = UUIDGenerator.getCurrentUUID()) < maxRecoverId) {
            if (UUIDGenerator.setUUID(currentId, maxRecoverId)) {
                break;
            }
        }
    }

    private void restore(TransactionWriteStore store) {
    }

    @Override
    public void destroy() {
        transactionStoreManager.shutdown();
    }
}
