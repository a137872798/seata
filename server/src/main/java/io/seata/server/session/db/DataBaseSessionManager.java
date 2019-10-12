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
package io.seata.server.session.db;

import java.util.Collection;
import java.util.List;

import io.seata.common.exception.StoreException;
import io.seata.common.executor.Initialize;
import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.common.loader.LoadLevel;
import io.seata.common.util.StringUtils;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.GlobalStatus;
import io.seata.core.store.StoreMode;
import io.seata.server.session.AbstractSessionManager;
import io.seata.server.session.BranchSession;
import io.seata.server.session.GlobalSession;
import io.seata.server.session.SessionCondition;
import io.seata.server.session.SessionHolder;
import io.seata.server.session.SessionLifecycleListener;
import io.seata.server.session.SessionManager;
import io.seata.server.store.TransactionStoreManager;
import io.seata.server.store.TransactionStoreManager.LogOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Data base session manager.
 * 基于DB 的sessionManager
 * @author zhangsen
 * @data 2019 /4/4
 */
@LoadLevel(name = "db")
public class DataBaseSessionManager extends AbstractSessionManager
    implements SessionManager, SessionLifecycleListener, Initialize {

    /**
     * The constant LOGGER.
     */
    protected static final Logger LOGGER = LoggerFactory.getLogger(DataBaseSessionManager.class);

    /**
     * The Task name.
     */
    protected String taskName;

    /**
     * Instantiates a new Data base session manager.
     */
    public DataBaseSessionManager() {
        super();
    }

    /**
     * Instantiates a new Data base session manager.
     * SessionHolder 中有4个该对象 分别用于 同步/异步/重试/回滚
     * @param name the name
     */
    public DataBaseSessionManager(String name) {
        super();
        this.taskName = name;
    }

    /**
     * 基于SPI的加载 会触发 init 方法
     */
    @Override
    public void init() {
        // transactionStoreManager 对象是 持久化数据的对象 SessionManager 本身的保存逻辑实际上是委托给该对象
        transactionStoreManager = EnhancedServiceLoader.load(TransactionStoreManager.class, StoreMode.DB.name());
    }

    // 下面的操作 基本都会涉及到taskName的判断

    /**
     * @param session the session
     * @throws TransactionException
     */
    @Override
    public void addGlobalSession(GlobalSession session) throws TransactionException {
        // 推测 未设置名字 就代表是同步处理 那么 session 处理完后肯定是删除了  这里就要重新添加
        if (StringUtils.isBlank(taskName)) {
            boolean ret = transactionStoreManager.writeSession(LogOperation.GLOBAL_ADD, session);
            if (!ret) {
                throw new StoreException("addGlobalSession failed.");
            }
        } else {
            // 异步模式 可能全局事务的记录还在数据库中 那么就进行更新
            boolean ret = transactionStoreManager.writeSession(LogOperation.GLOBAL_UPDATE, session);
            if (!ret) {
                throw new StoreException("addGlobalSession failed.");
            }
        }
    }

    /**
     * 异步模式才有更新的必要
     * @param session the session
     * @param status  the status
     * @throws TransactionException
     */
    @Override
    public void updateGlobalSessionStatus(GlobalSession session, GlobalStatus status) throws TransactionException {
        if (StringUtils.isNotBlank(taskName)) {
            return;
        }
        session.setStatus(status);
        boolean ret = transactionStoreManager.writeSession(LogOperation.GLOBAL_UPDATE, session);
        if (!ret) {
            throw new StoreException("updateGlobalSessionStatus failed.");
        }
    }

    @Override
    public void removeGlobalSession(GlobalSession session) throws TransactionException {
        if (StringUtils.isNotBlank(taskName)) {
            return;
        }
        boolean ret = transactionStoreManager.writeSession(LogOperation.GLOBAL_REMOVE, session);
        if (!ret) {
            throw new StoreException("removeGlobalSession failed.");
        }
    }

    @Override
    public void addBranchSession(GlobalSession globalSession, BranchSession session) throws TransactionException {
        if (StringUtils.isNotBlank(taskName)) {
            return;
        }
        boolean ret = transactionStoreManager.writeSession(LogOperation.BRANCH_ADD, session);
        if (!ret) {
            throw new StoreException("addBranchSession failed.");
        }
    }

    @Override
    public void updateBranchSessionStatus(BranchSession session, BranchStatus status) throws TransactionException {
        if (StringUtils.isNotBlank(taskName)) {
            return;
        }
        boolean ret = transactionStoreManager.writeSession(LogOperation.BRANCH_UPDATE, session);
        if (!ret) {
            throw new StoreException("updateBranchSessionStatus failed.");
        }
    }

    @Override
    public void removeBranchSession(GlobalSession globalSession, BranchSession session) throws TransactionException {
        if (StringUtils.isNotBlank(taskName)) {
            return;
        }
        boolean ret = transactionStoreManager.writeSession(LogOperation.BRANCH_REMOVE, session);
        if (!ret) {
            throw new StoreException("removeBranchSession failed.");
        }
    }

    @Override
    public GlobalSession findGlobalSession(String xid) {
        return transactionStoreManager.readSession(xid);
    }

    @Override
    public Collection<GlobalSession> allSessions() {
        //get by taskName  根据情况寻找不同的 session  SessionCondition 代表搜寻session 的条件对象
        if (SessionHolder.ASYNC_COMMITTING_SESSION_MANAGER_NAME.equalsIgnoreCase(taskName)) {
            return findGlobalSessions(new SessionCondition(GlobalStatus.AsyncCommitting));
        } else if (SessionHolder.RETRY_COMMITTING_SESSION_MANAGER_NAME.equalsIgnoreCase(taskName)) {
            return findGlobalSessions(new SessionCondition(new GlobalStatus[] {GlobalStatus.CommitRetrying}));
        } else if (SessionHolder.RETRY_ROLLBACKING_SESSION_MANAGER_NAME.equalsIgnoreCase(taskName)) {
            return findGlobalSessions(new SessionCondition(new GlobalStatus[] {GlobalStatus.RollbackRetrying,
                GlobalStatus.TimeoutRollbacking, GlobalStatus.TimeoutRollbackRetrying}));
        } else {
            //all data
            return findGlobalSessions(new SessionCondition(new GlobalStatus[] {
                GlobalStatus.UnKnown, GlobalStatus.Begin,
                GlobalStatus.Committing, GlobalStatus.CommitRetrying, GlobalStatus.Rollbacking,
                GlobalStatus.RollbackRetrying,
                GlobalStatus.TimeoutRollbacking, GlobalStatus.TimeoutRollbackRetrying, GlobalStatus.AsyncCommitting}));
        }
    }

    @Override
    public List<GlobalSession> findGlobalSessions(SessionCondition condition) {
        //nothing need to do
        return transactionStoreManager.readSession(condition);
    }

}