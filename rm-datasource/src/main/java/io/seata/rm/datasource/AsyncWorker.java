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
package io.seata.rm.datasource;

import com.alibaba.druid.util.JdbcConstants;
import io.seata.common.exception.NotSupportYetException;
import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.CollectionUtils;
import io.seata.config.ConfigurationFactory;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.BranchType;
import io.seata.core.model.ResourceManagerInbound;
import io.seata.rm.DefaultResourceManager;
import io.seata.rm.datasource.undo.UndoLogManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.seata.core.constants.ConfigurationKeys.CLIENT_ASYNC_COMMIT_BUFFER_LIMIT;

/**
 * The type Async worker.
 * 异步工作者对象 实现了 RMInbound 对象 rm 处理 commit 和rollback的 逻辑都在这里实现
 * @author sharajava
 */
public class AsyncWorker implements ResourceManagerInbound {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncWorker.class);

    private static final int DEFAULT_RESOURCE_SIZE = 16;

    private static final int UNDOLOG_DELETE_LIMIT_SIZE = 1000;

    /**
     * 2阶段事务提交上下文
     */
    private static class Phase2Context {

        /**
         * Instantiates a new Phase 2 context.
         *
         * @param branchType      the branchType
         * @param xid             the xid
         * @param branchId        the branch id
         * @param resourceId      the resource id
         * @param applicationData the application data
         */
        public Phase2Context(BranchType branchType, String xid, long branchId, String resourceId, String applicationData) {
            this.xid = xid;
            this.branchId = branchId;
            this.resourceId = resourceId;
            this.applicationData = applicationData;
            this.branchType = branchType;
        }

        /**
         * The Xid.
         */
        String xid;
        /**
         * The Branch id.
         */
        long branchId;
        /**
         * The Resource id.
         */
        String resourceId;
        /**
         * The Application data.
         * 应用数据 难道是序列化过的???
         */
        String applicationData;

        /**
         * the branch Type
         */
        BranchType branchType;
    }

    private static int ASYNC_COMMIT_BUFFER_LIMIT = ConfigurationFactory.getInstance().getInt(
            CLIENT_ASYNC_COMMIT_BUFFER_LIMIT, 10000);

    /**
     * 创建阻塞队列
     */
    private static final BlockingQueue<Phase2Context> ASYNC_COMMIT_BUFFER = new LinkedBlockingQueue<>(ASYNC_COMMIT_BUFFER_LIMIT);


    /**
     * 定时器对象
     */
    private static ScheduledExecutorService timerExecutor;

    @Override
    public BranchStatus branchCommit(BranchType branchType, String xid, long branchId, String resourceId, String applicationData) throws TransactionException {
        // 当无法提交任务时 提示异常信息
        if (!ASYNC_COMMIT_BUFFER.offer(new Phase2Context(branchType, xid, branchId, resourceId, applicationData))) {
            LOGGER.warn("Async commit buffer is FULL. Rejected branch [" + branchId + "/" + xid + "] will be handled by housekeeping later.");
        }
        return BranchStatus.PhaseTwo_Committed;
    }

    /**
     * Init.
     * 初始化线程池
     */
    public synchronized void init() {
        LOGGER.info("Async Commit Buffer Limit: " + ASYNC_COMMIT_BUFFER_LIMIT);
        timerExecutor = new ScheduledThreadPoolExecutor(1,
                new NamedThreadFactory("AsyncWorker", 1, true));
        timerExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {

                    // 该线程池 专门执行commit 任务
                    doBranchCommits();

                } catch (Throwable e) {
                    LOGGER.info("Failed at async committing ... " + e.getMessage());

                }
            }
        }, 10, 1000 * 1, TimeUnit.MILLISECONDS);
    }

    /**
     * 消费阻塞队列中的 提交任务
     * 这里怎么没有看到提交动作 只是看到了删除日志的动作 ???
     */
    private void doBranchCommits() {
        if (ASYNC_COMMIT_BUFFER.size() == 0) {
            return;
        }

        Map<String, List<Phase2Context>> mappedContexts = new HashMap<>(DEFAULT_RESOURCE_SIZE);

        // 每次都会处理所有的任务  这里先将队列中所有任务设置到 map中
        while (!ASYNC_COMMIT_BUFFER.isEmpty()) {
            Phase2Context commitContext = ASYNC_COMMIT_BUFFER.poll();
            // 将任务队列中所有元素取出来后 将其设置到一个map 中 string 代表同属一个资源(事务)  上下文对象推测就是分布式事务中各个分支事务
            List<Phase2Context> contextsGroupedByResourceId = mappedContexts.get(commitContext.resourceId);
            if (contextsGroupedByResourceId == null) {
                contextsGroupedByResourceId = new ArrayList<>();
                mappedContexts.put(commitContext.resourceId, contextsGroupedByResourceId);
            }
            contextsGroupedByResourceId.add(commitContext);

        }

        // 遍历上面生成的map 实际上可以用一个流函数去操作吧
        for (Map.Entry<String, List<Phase2Context>> entry : mappedContexts.entrySet()) {
            Connection conn = null;
            DataSourceProxy dataSourceProxy;
            try {
                try {
                    // 获取处理AT 的 manager 对象
                    DataSourceManager resourceManager = (DataSourceManager) DefaultResourceManager.get().getResourceManager(BranchType.AT);
                    // 每个 key 都有自己的 dataSource 对象 他们被缓存在 DataSourceManager 中
                    dataSourceProxy = resourceManager.get(entry.getKey());
                    if (dataSourceProxy == null) {
                        throw new ShouldNeverHappenException("Failed to find resource on " + entry.getKey());
                    }
                    // 获取真正的连接对象
                    conn = dataSourceProxy.getPlainConnection();
                } catch (SQLException sqle) {
                    LOGGER.warn("Failed to get connection for async committing on " + entry.getKey(), sqle);
                    continue;
                }
                // 获取某个 分布式事务中所有提交的上下文信息 每个都代表一个 branch的信息
                List<Phase2Context> contextsGroupedByResourceId = entry.getValue();
                // 代表准备删除的 操作日志
                Set<String> xids = new LinkedHashSet<>(UNDOLOG_DELETE_LIMIT_SIZE);
                Set<Long> branchIds = new LinkedHashSet<>(UNDOLOG_DELETE_LIMIT_SIZE);
                for (Phase2Context commitContext : contextsGroupedByResourceId) {
                    xids.add(commitContext.xid);
                    branchIds.add(commitContext.branchId);
                    int maxSize = xids.size() > branchIds.size() ? xids.size() : branchIds.size();
                    // 如果已经满了 立即删除 而不采用扩容的方式
                    if(maxSize == UNDOLOG_DELETE_LIMIT_SIZE){
                        try {
                            // 删除 操作日志
                            UndoLogManagerFactory.getUndoLogManager(dataSourceProxy.getDbType()).batchDeleteUndoLog(xids, branchIds, conn);
                        } catch (Exception ex) {
                            LOGGER.warn("Failed to batch delete undo log [" + branchIds + "/" + xids + "]", ex);
                        }
                        xids.clear();
                        branchIds.clear();
                    }
                }

                if (CollectionUtils.isEmpty(xids) || CollectionUtils.isEmpty(branchIds)) {
                    return;
                }

                try {
                    UndoLogManagerFactory.getUndoLogManager(dataSourceProxy.getDbType()).batchDeleteUndoLog(xids, branchIds, conn);
                }catch (Exception ex) {
                    LOGGER.warn("Failed to batch delete undo log [" + branchIds + "/" + xids + "]", ex);
                }

            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException closeEx) {
                        LOGGER.warn("Failed to close JDBC resource while deleting undo_log ", closeEx);
                    }
                }
            }
        }
    }

    /**
     * 不支持回滚
     * @param branchType      the branch type
     * @param xid             Transaction id.
     * @param branchId        Branch id.
     * @param resourceId      Resource id.
     * @param applicationData Application data bind with this branch.
     * @return
     * @throws TransactionException
     */
    @Override
    public BranchStatus branchRollback(BranchType branchType, String xid, long branchId, String resourceId, String applicationData) throws TransactionException {
        throw new NotSupportYetException();

    }
}
