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
package io.seata.server.session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.exception.StoreException;
import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.common.util.StringUtils;
import io.seata.config.Configuration;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.GlobalStatus;
import io.seata.core.store.StoreMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Session holder.
 * @author sharajava
 */
public class SessionHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionHolder.class);

    /**
     * The constant CONFIG.
     * 单例对象
     */
    protected static final Configuration CONFIG = ConfigurationFactory.getInstance();

    /**
     * The constant DEFAULT.
     */
    public static final String DEFAULT = "default";

    /**
     * The constant ROOT_SESSION_MANAGER_NAME.
     */
    public static final String ROOT_SESSION_MANAGER_NAME = "root.data";
    /**
     * The constant ASYNC_COMMITTING_SESSION_MANAGER_NAME.
     */
    public static final String ASYNC_COMMITTING_SESSION_MANAGER_NAME = "async.commit.data";
    /**
     * The constant RETRY_COMMITTING_SESSION_MANAGER_NAME.
     */
    public static final String RETRY_COMMITTING_SESSION_MANAGER_NAME = "retry.commit.data";
    /**
     * The constant RETRY_ROLLBACKING_SESSION_MANAGER_NAME.
     */
    public static final String RETRY_ROLLBACKING_SESSION_MANAGER_NAME = "retry.rollback.data";

    /**
     * The default session store dir
     */
    public static final String DEFAULT_SESSION_STORE_FILE_DIR = "sessionStore";

    private static SessionManager ROOT_SESSION_MANAGER;
    private static SessionManager ASYNC_COMMITTING_SESSION_MANAGER;
    private static SessionManager RETRY_COMMITTING_SESSION_MANAGER;
    private static SessionManager RETRY_ROLLBACKING_SESSION_MANAGER;

    /**
     * Init.
     * 根据使用的 持久化方式进行初始化  每个server 都需要具备持久化client信息的功能 否则一旦server出现问题 重新恢复后会丢失之前的会话
     * @param mode the store mode: file、db
     * @throws IOException the io exception
     */
    public static void init(String mode) throws IOException {
        // 如果没有指定存储模式 尝试从配置中心获取
        if (StringUtils.isBlank(mode)) {
            //use default
            // 获取存储模式
            mode = CONFIG.getConfig(ConfigurationKeys.STORE_MODE);
        }
        //the store mode
        StoreMode storeMode = StoreMode.valueof(mode);
        // 如果是基于 db 的存储
        if (StoreMode.DB.equals(storeMode)) {
            //database store
            // 获取基于 DB 的 SessionManager 对象
            ROOT_SESSION_MANAGER = EnhancedServiceLoader.load(SessionManager.class, StoreMode.DB.name());
            // 获取异步处理对象  这里传入额外参数 代表使用指定的构造函数进行初始化
            ASYNC_COMMITTING_SESSION_MANAGER = EnhancedServiceLoader.load(SessionManager.class, StoreMode.DB.name(),
                new Object[] {ASYNC_COMMITTING_SESSION_MANAGER_NAME});
            // 获取重试对象
            RETRY_COMMITTING_SESSION_MANAGER = EnhancedServiceLoader.load(SessionManager.class, StoreMode.DB.name(),
                new Object[] {RETRY_COMMITTING_SESSION_MANAGER_NAME});
            // 回滚对象
            RETRY_ROLLBACKING_SESSION_MANAGER = EnhancedServiceLoader.load(SessionManager.class, StoreMode.DB.name(),
                new Object[] {RETRY_ROLLBACKING_SESSION_MANAGER_NAME});
        // 基于文件的持久化
        } else if (StoreMode.FILE.equals(storeMode)) {
            //file store 获取文件存储路径
            String sessionStorePath = CONFIG.getConfig(ConfigurationKeys.STORE_FILE_DIR, DEFAULT_SESSION_STORE_FILE_DIR);
            if (sessionStorePath == null) {
                throw new StoreException("the {store.file.dir} is empty.");
            }
            ROOT_SESSION_MANAGER = EnhancedServiceLoader.load(SessionManager.class, StoreMode.FILE.name(),
                new Object[] {ROOT_SESSION_MANAGER_NAME, sessionStorePath});
            // 这里剩余的3种模式 创建的是 Default   DefaultSessionManager 实际上都是将数据存放在堆中
            ASYNC_COMMITTING_SESSION_MANAGER = EnhancedServiceLoader.load(SessionManager.class, DEFAULT,
                new Object[] {ASYNC_COMMITTING_SESSION_MANAGER_NAME});
            RETRY_COMMITTING_SESSION_MANAGER = EnhancedServiceLoader.load(SessionManager.class, DEFAULT,
                new Object[] {RETRY_COMMITTING_SESSION_MANAGER_NAME});
            RETRY_ROLLBACKING_SESSION_MANAGER = EnhancedServiceLoader.load(SessionManager.class, DEFAULT,
                new Object[] {RETRY_ROLLBACKING_SESSION_MANAGER_NAME});
        } else {
            //unknown store
            throw new IllegalArgumentException("unknown store mode:" + mode);
        }
        //relaod
        // 一般在server 启动时会创建该对象 这时 server 可能是重启过的 那么就需要加载宕机前已经保存的数据
        reload();
    }

    /**
     * Reload.
     * 加载数据
     */
    protected static void reload() {
        // 首先确保 SessionManager 是可加载对象  目前只有基于文件的 sessionManager是 reloadable
        if (ROOT_SESSION_MANAGER instanceof Reloadable) {
            // 触发 manager 的 reload 方法
            ((Reloadable)ROOT_SESSION_MANAGER).reload();

            // 重新加载之后 获取了上次没有处理完的 全局事务
            Collection<GlobalSession> reloadedSessions = ROOT_SESSION_MANAGER.allSessions();
            // 如果 session 本身不为空
            if (reloadedSessions != null && !reloadedSessions.isEmpty()) {
                reloadedSessions.forEach(globalSession -> {
                    GlobalStatus globalStatus = globalSession.getStatus();
                    switch (globalStatus) {
                        // 不允许出现这些情况 因为这些代表已经处理好了
                        case UnKnown:
                        case Committed:
                        case CommitFailed:
                        case Rollbacked:
                        case RollbackFailed:
                        case TimeoutRollbacked:
                        case TimeoutRollbackFailed:
                        case Finished:
                            throw new ShouldNeverHappenException("Reloaded Session should NOT be " + globalStatus);
                        // 异步提交
                        case AsyncCommitting:
                            try {
                                // 为全局session 设置 asyncCommitSessionManager 作为监听器  该SessionManager 会在全局事务在对应的生命周期时 触发对应的钩子函数
                                // 比如 全局事务被创建时 进行持久化 触发移除时 从DB/File 中移除
                                globalSession.addSessionLifecycleListener(getAsyncCommittingSessionManager());
                                // 触发增加的钩子
                                getAsyncCommittingSessionManager().addGlobalSession(globalSession);
                            } catch (TransactionException e) {
                                throw new ShouldNeverHappenException(e);
                            }
                            break;
                        // 比如 回滚 同步提交等
                        default: {
                            // 重启时 要为 之前的分事务全部上锁
                            ArrayList<BranchSession> branchSessions = globalSession.getSortedBranches();
                            // Lock
                            branchSessions.forEach(branchSession -> {
                                try {
                                    // 对分事务进行加锁  基于DB 的实现就是在 数据库中添加了数据
                                    branchSession.lock();
                                } catch (TransactionException e) {
                                    throw new ShouldNeverHappenException(e);
                                }
                            });

                            switch (globalStatus) {
                                case Committing:
                                case CommitRetrying:
                                    try {
                                        // 增加重试监听器
                                        globalSession.addSessionLifecycleListener(
                                            getRetryCommittingSessionManager());
                                        getRetryCommittingSessionManager().addGlobalSession(globalSession);
                                    } catch (TransactionException e) {
                                        throw new ShouldNeverHappenException(e);
                                    }
                                    break;
                                case Rollbacking:
                                case RollbackRetrying:
                                case TimeoutRollbacking:
                                case TimeoutRollbackRetrying:
                                    try {
                                        // 增加 回滚监听器
                                        globalSession.addSessionLifecycleListener(
                                            getRetryRollbackingSessionManager());
                                        getRetryRollbackingSessionManager().addGlobalSession(globalSession);
                                    } catch (TransactionException e) {
                                        throw new ShouldNeverHappenException(e);
                                    }
                                    break;
                                case Begin:
                                    // begin 代表需要设置成 活跃状态
                                    globalSession.setActive(true);
                                    break;
                                default:
                                    throw new ShouldNeverHappenException("NOT properly handled " + globalStatus);
                            }

                            break;

                        }
                    }

                });
            }
        }
    }

    /**
     * Gets root session manager.
     *
     * @return the root session manager
     */
    public static final SessionManager getRootSessionManager() {
        if (ROOT_SESSION_MANAGER == null) {
            throw new ShouldNeverHappenException("SessionManager is NOT init!");
        }
        return ROOT_SESSION_MANAGER;
    }

    /**
     * Gets async committing session manager.
     *
     * @return the async committing session manager
     */
    public static final SessionManager getAsyncCommittingSessionManager() {
        if (ASYNC_COMMITTING_SESSION_MANAGER == null) {
            throw new ShouldNeverHappenException("SessionManager is NOT init!");
        }
        return ASYNC_COMMITTING_SESSION_MANAGER;
    }

    /**
     * Gets retry committing session manager.
     *
     * @return the retry committing session manager
     */
    public static final SessionManager getRetryCommittingSessionManager() {
        if (RETRY_COMMITTING_SESSION_MANAGER == null) {
            throw new ShouldNeverHappenException("SessionManager is NOT init!");
        }
        return RETRY_COMMITTING_SESSION_MANAGER;
    }

    /**
     * Gets retry rollbacking session manager.
     *
     * @return the retry rollbacking session manager
     */
    public static final SessionManager getRetryRollbackingSessionManager() {
        if (RETRY_ROLLBACKING_SESSION_MANAGER == null) {
            throw new ShouldNeverHappenException("SessionManager is NOT init!");
        }
        return RETRY_ROLLBACKING_SESSION_MANAGER;
    }

    /**
     * Find global session.
     * 获取全局session
     * @param xid the xid
     * @return the global session
     */
    public static GlobalSession findGlobalSession(String xid) {
        return getRootSessionManager().findGlobalSession(xid);
    }

    public static void destory() {
        ROOT_SESSION_MANAGER.destroy();
        ASYNC_COMMITTING_SESSION_MANAGER.destroy();
        RETRY_COMMITTING_SESSION_MANAGER.destroy();
        RETRY_ROLLBACKING_SESSION_MANAGER.destroy();
    }
}
