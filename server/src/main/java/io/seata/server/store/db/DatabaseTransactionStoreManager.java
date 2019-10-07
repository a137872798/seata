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
package io.seata.server.store.db;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import io.seata.common.exception.StoreException;
import io.seata.common.executor.Initialize;
import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.common.loader.LoadLevel;
import io.seata.common.util.CollectionUtils;
import io.seata.common.util.StringUtils;
import io.seata.config.Configuration;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.BranchType;
import io.seata.core.model.GlobalStatus;
import io.seata.core.store.BranchTransactionDO;
import io.seata.core.store.GlobalTransactionDO;
import io.seata.core.store.LogStore;
import io.seata.core.store.StoreMode;
import io.seata.core.store.db.DataSourceGenerator;
import io.seata.server.session.BranchSession;
import io.seata.server.session.GlobalSession;
import io.seata.server.session.SessionCondition;
import io.seata.server.store.AbstractTransactionStoreManager;
import io.seata.server.store.SessionStorable;
import io.seata.server.store.TransactionStoreManager;

/**
 * The type Database transaction store manager.
 * 基于DB 的  TSM 对象
 * @author zhangsen
 * @data 2019 /4/2
 */
@LoadLevel(name = "db")
public class DatabaseTransactionStoreManager extends AbstractTransactionStoreManager
    implements TransactionStoreManager, Initialize {

    /**
     * The constant CONFIG.
     */
    protected static final Configuration CONFIG = ConfigurationFactory.getInstance();

    /**
     * The constant DEFAULT_LOG_QUERY_LIMIT.
     */
    protected static final int DEFAULT_LOG_QUERY_LIMIT = 100;

    /**
     * is inited
     */
    protected AtomicBoolean inited = new AtomicBoolean(false);

    /**
     * The Log store.
     */
    protected LogStore logStore;

    /**
     * The Log query limit.
     * 查询条数限制
     */
    protected int logQueryLimit;

    /**
     * Instantiates a new Database transaction store manager.
     */
    public DatabaseTransactionStoreManager() {
    }

    /**
     * 初始化 TSM 对象
     */
    @Override
    public synchronized void init() {
        // 如果已经完成初始化 直接返回
        if (inited.get()) {
            return;
        }
        // 获取查询限制
        logQueryLimit = CONFIG.getInt(ConfigurationKeys.STORE_DB_LOG_QUERY_LIMIT, DEFAULT_LOG_QUERY_LIMIT);
        // 获取数据库类型
        String datasourceType = CONFIG.getConfig(ConfigurationKeys.STORE_DB_DATASOURCE_TYPE);
        //init dataSource
        // 通过SPI机制加载dataSource 生成器对象
        DataSourceGenerator dataSourceGenerator = EnhancedServiceLoader.load(DataSourceGenerator.class, datasourceType);
        // 生成 DataSource 对象
        DataSource logStoreDataSource = dataSourceGenerator.generateDataSource();
        // 获取访问 globalSession 的基础接口对象
        logStore = EnhancedServiceLoader.load(LogStore.class, StoreMode.DB.name(), new Class[] {DataSource.class},
            new Object[] {logStoreDataSource});
        inited.set(true);
    }

    /**
     * 写入 session
     * @param logOperation the log operation
     * @param session      the session
     * @return
     */
    @Override
    public boolean writeSession(LogOperation logOperation, SessionStorable session) {
        // 通过 operationType 执行不同的操作
        if (LogOperation.GLOBAL_ADD.equals(logOperation)) {
            logStore.insertGlobalTransactionDO(convertGlobalTransactionDO(session));
        } else if (LogOperation.GLOBAL_UPDATE.equals(logOperation)) {
            logStore.updateGlobalTransactionDO(convertGlobalTransactionDO(session));
        } else if (LogOperation.GLOBAL_REMOVE.equals(logOperation)) {
            logStore.deleteGlobalTransactionDO(convertGlobalTransactionDO(session));
        } else if (LogOperation.BRANCH_ADD.equals(logOperation)) {
            logStore.insertBranchTransactionDO(convertBranchTransactionDO(session));
        } else if (LogOperation.BRANCH_UPDATE.equals(logOperation)) {
            logStore.updateBranchTransactionDO(convertBranchTransactionDO(session));
        } else if (LogOperation.BRANCH_REMOVE.equals(logOperation)) {
            logStore.deleteBranchTransactionDO(convertBranchTransactionDO(session));
        } else {
            throw new StoreException("Unknown LogOperation:" + logOperation.name());
        }
        return true;
    }

    /**
     * Read session global session.
     * 通过事务id 定位到某个全局事务对象上
     * @param transactionId the transaction id
     * @return the global session
     */
    public GlobalSession readSession(Long transactionId) {
        //global transaction
        GlobalTransactionDO globalTransactionDO = logStore.queryGlobalTransactionDO(transactionId);
        if (globalTransactionDO == null) {
            return null;
        }
        //branch transactions
        List<BranchTransactionDO> branchTransactionDOs = logStore.queryBranchTransactionDO(
            globalTransactionDO.getXid());
        // 看来 session 对象是由  globalTransactionDO + branchTransactionDO 组合成的
        return getGlobalSession(globalTransactionDO, branchTransactionDOs);
    }

    /**
     * Read session global session.
     *
     * @param xid the xid
     * @return the global session
     */
    @Override
    public GlobalSession readSession(String xid) {
        //global transaction
        GlobalTransactionDO globalTransactionDO = logStore.queryGlobalTransactionDO(xid);
        if (globalTransactionDO == null) {
            return null;
        }
        //branch transactions
        List<BranchTransactionDO> branchTransactionDOs = logStore.queryBranchTransactionDO(
            globalTransactionDO.getXid());
        return getGlobalSession(globalTransactionDO, branchTransactionDOs);
    }

    /**
     * Read session list.
     * 通过一组状态对象查询对应状态的 globalSession 对象
     * @param statuses the statuses
     * @return the list
     */
    public List<GlobalSession> readSession(GlobalStatus[] statuses) {
        int[] states = new int[statuses.length];
        for (int i = 0; i < statuses.length; i++) {
            states[i] = statuses[i].getCode();
        }
        // 获取全局事务对象
        List<GlobalSession> globalSessions = new ArrayList<GlobalSession>();
        //global transaction
        // 通过状态和查询条数 限制结果集
        List<GlobalTransactionDO> globalTransactionDOs = logStore.queryGlobalTransactionDO(states, logQueryLimit);
        if (CollectionUtils.isEmpty(globalTransactionDOs)) {
            return null;
        }
        // 获取返回的所有全局事务对象
        for (GlobalTransactionDO globalTransactionDO : globalTransactionDOs) {
            // 通过事务id 从总事务对象中获取 分事务
            List<BranchTransactionDO> branchTransactionDOs = logStore.queryBranchTransactionDO(
                globalTransactionDO.getXid());
            globalSessions.add(getGlobalSession(globalTransactionDO, branchTransactionDOs));
        }
        return globalSessions;
    }

    /**
     * 根据条件查询 全局session 对象
     * @param sessionCondition the session condition
     * @return
     */
    @Override
    public List<GlobalSession> readSession(SessionCondition sessionCondition) {
        if (StringUtils.isNotBlank(sessionCondition.getXid())) {
            GlobalSession globalSession = readSession(sessionCondition.getXid());
            if (globalSession != null) {
                List<GlobalSession> globalSessions = new ArrayList<>();
                globalSessions.add(globalSession);
                return globalSessions;
            }
        } else if (sessionCondition.getTransactionId() != null) {
            GlobalSession globalSession = readSession(sessionCondition.getTransactionId());
            if (globalSession != null) {
                List<GlobalSession> globalSessions = new ArrayList<>();
                globalSessions.add(globalSession);
                return globalSessions;
            }
        } else if (CollectionUtils.isNotEmpty(sessionCondition.getStatuses())) {
            return readSession(sessionCondition.getStatuses());
        }
        return null;
    }

    /**
     * 将 globalTransactionDO 与 branchTransactionDO 组合成 session 对象
     * @param globalTransactionDO
     * @param branchTransactionDOs
     * @return
     */
    private GlobalSession getGlobalSession(GlobalTransactionDO globalTransactionDO,
                                           List<BranchTransactionDO> branchTransactionDOs) {
        GlobalSession globalSession = convertGlobalSession(globalTransactionDO);
        //branch transactions
        if (branchTransactionDOs != null && branchTransactionDOs.size() > 0) {
            for (BranchTransactionDO branchTransactionDO : branchTransactionDOs) {
                globalSession.add(convertBranchSession(branchTransactionDO));
            }
        }
        return globalSession;
    }

    private GlobalSession convertGlobalSession(GlobalTransactionDO globalTransactionDO) {
        GlobalSession session = new GlobalSession(globalTransactionDO.getApplicationId(),
            globalTransactionDO.getTransactionServiceGroup(),
            globalTransactionDO.getTransactionName(),
            globalTransactionDO.getTimeout());
        session.setTransactionId(globalTransactionDO.getTransactionId());
        session.setXid(globalTransactionDO.getXid());
        session.setStatus(GlobalStatus.get(globalTransactionDO.getStatus()));
        session.setApplicationData(globalTransactionDO.getApplicationData());
        session.setBeginTime(globalTransactionDO.getBeginTime());
        return session;
    }

    private BranchSession convertBranchSession(BranchTransactionDO branchTransactionDO) {
        BranchSession branchSession = new BranchSession();
        branchSession.setXid(branchTransactionDO.getXid());
        branchSession.setTransactionId(branchTransactionDO.getTransactionId());
        branchSession.setApplicationData(branchTransactionDO.getApplicationData());
        branchSession.setBranchId(branchTransactionDO.getBranchId());
        branchSession.setBranchType(BranchType.valueOf(branchTransactionDO.getBranchType()));
        branchSession.setResourceId(branchTransactionDO.getResourceId());
        branchSession.setClientId(branchTransactionDO.getClientId());
        branchSession.setLockKey(branchTransactionDO.getLockKey());
        branchSession.setResourceGroupId(branchTransactionDO.getResourceGroupId());
        branchSession.setStatus(BranchStatus.get(branchTransactionDO.getStatus()));
        return branchSession;
    }

    /**
     * 将可存储的 session 对象保存到数据库中
     * @param session
     * @return
     */
    private GlobalTransactionDO convertGlobalTransactionDO(SessionStorable session) {
        // 要求调用该方法的必须是 GlobalSession 对象
        if (session == null || !(session instanceof GlobalSession)) {
            throw new IllegalArgumentException(
                "the parameter of SessionStorable is not available, SessionStorable:" + StringUtils.toString(session));
        }
        GlobalSession globalSession = (GlobalSession)session;

        // 转换成 DO 对象
        GlobalTransactionDO globalTransactionDO = new GlobalTransactionDO();
        globalTransactionDO.setXid(globalSession.getXid());
        globalTransactionDO.setStatus(globalSession.getStatus().getCode());
        globalTransactionDO.setApplicationId(globalSession.getApplicationId());
        globalTransactionDO.setBeginTime(globalSession.getBeginTime());
        globalTransactionDO.setTimeout(globalSession.getTimeout());
        globalTransactionDO.setTransactionId(globalSession.getTransactionId());
        globalTransactionDO.setTransactionName(globalSession.getTransactionName());
        globalTransactionDO.setTransactionServiceGroup(globalSession.getTransactionServiceGroup());
        globalTransactionDO.setApplicationData(globalSession.getApplicationData());
        return globalTransactionDO;
    }

    private BranchTransactionDO convertBranchTransactionDO(SessionStorable session) {
        if (session == null || !(session instanceof BranchSession)) {
            throw new IllegalArgumentException(
                "the parameter of SessionStorable is not available, SessionStorable:" + StringUtils.toString(session));
        }
        BranchSession branchSession = (BranchSession)session;

        BranchTransactionDO branchTransactionDO = new BranchTransactionDO();
        branchTransactionDO.setXid(branchSession.getXid());
        branchTransactionDO.setBranchId(branchSession.getBranchId());
        branchTransactionDO.setBranchType(branchSession.getBranchType().name());
        branchTransactionDO.setClientId(branchSession.getClientId());
        branchTransactionDO.setLockKey(branchSession.getLockKey());
        branchTransactionDO.setResourceGroupId(branchSession.getResourceGroupId());
        branchTransactionDO.setTransactionId(branchSession.getTransactionId());
        branchTransactionDO.setApplicationData(branchSession.getApplicationData());
        branchTransactionDO.setResourceId(branchSession.getResourceId());
        branchTransactionDO.setStatus(branchSession.getStatus().getCode());
        return branchTransactionDO;
    }

    /**
     * Sets log store.
     *
     * @param logStore the log store
     */
    public void setLogStore(LogStore logStore) {
        this.logStore = logStore;
    }

    /**
     * Sets log query limit.
     *
     * @param logQueryLimit the log query limit
     */
    public void setLogQueryLimit(int logQueryLimit) {
        this.logQueryLimit = logQueryLimit;
    }
}