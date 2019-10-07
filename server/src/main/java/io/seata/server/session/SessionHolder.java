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
 * 维护各种单例 保证对应的 manager 是全局唯一的
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
     * 进行初始化
     * @param mode the store mode: file、db
     * @throws IOException the io exception
     */
    public static void init(String mode) throws IOException {
        if (StringUtils.isBlank(mode)) {
            //use default
            // 获取存储模式
            mode = CONFIG.getConfig(ConfigurationKeys.STORE_MODE);
        }
        //the store mode
        // 有基于 DB 的 存储模式 还有基于 File 的存储模式
        StoreMode storeMode = StoreMode.valueof(mode);
        // 如果是db模式
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
        } else if (StoreMode.FILE.equals(storeMode)) {
            //file store
            // 同上 只是实现变成了基于文件
            String sessionStorePath = CONFIG.getConfig(ConfigurationKeys.STORE_FILE_DIR, DEFAULT_SESSION_STORE_FILE_DIR);
            if (sessionStorePath == null) {
                throw new StoreException("the {store.file.dir} is empty.");
            }
            ROOT_SESSION_MANAGER = EnhancedServiceLoader.load(SessionManager.class, StoreMode.FILE.name(),
                new Object[] {ROOT_SESSION_MANAGER_NAME, sessionStorePath});
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
        // 加载某些数据
        reload();
    }

    /**
     * Reload.
     * 加载数据
     */
    protected static void reload() {
        // 首先确保 SessionManager 是可加载对象
        if (ROOT_SESSION_MANAGER instanceof Reloadable) {
            // 触发 manager 的 reload 方法
            ((Reloadable)ROOT_SESSION_MANAGER).reload();

            // 重新加载之后 获取当前所有session
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
                                // 为全局session 设置 asyncCommitSessionManager 作为监听器
                                globalSession.addSessionLifecycleListener(getAsyncCommittingSessionManager());
                                // 为 sm 对象增加 gs 对象
                                getAsyncCommittingSessionManager().addGlobalSession(globalSession);
                            } catch (TransactionException e) {
                                throw new ShouldNeverHappenException(e);
                            }
                            break;
                        // 其余代表初始状态
                        default: {
                            // 获取 该全局事务下的分支事务
                            ArrayList<BranchSession> branchSessions = globalSession.getSortedBranches();
                            // Lock
                            branchSessions.forEach(branchSession -> {
                                try {
                                    // 对分事务进行加锁
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
