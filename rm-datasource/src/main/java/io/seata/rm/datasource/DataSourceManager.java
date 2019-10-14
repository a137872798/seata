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

import io.seata.common.exception.FrameworkException;
import io.seata.common.exception.NotSupportYetException;
import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.executor.Initialize;
import io.seata.common.util.NetUtil;
import io.seata.core.context.RootContext;
import io.seata.core.exception.RmTransactionException;
import io.seata.core.exception.TransactionException;
import io.seata.core.exception.TransactionExceptionCode;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.BranchType;
import io.seata.core.model.Resource;
import io.seata.core.model.ResourceManagerInbound;
import io.seata.core.protocol.ResultCode;
import io.seata.core.protocol.transaction.GlobalLockQueryRequest;
import io.seata.core.protocol.transaction.GlobalLockQueryResponse;
import io.seata.core.rpc.netty.NettyClientConfig;
import io.seata.core.rpc.netty.RmRpcClient;
import io.seata.core.rpc.netty.TmRpcClient;
import io.seata.discovery.loadbalance.LoadBalanceFactory;
import io.seata.discovery.registry.RegistryFactory;
import io.seata.rm.AbstractResourceManager;
import io.seata.rm.datasource.undo.UndoLogManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import static io.seata.common.exception.FrameworkErrorCode.NoAvailableService;

/**
 * The type Data source manager.
 * 该对象 对应 AT 模式下的 资源管理器对象 在AT 模式下 每个dataSourceProxy 都会被看成一个Resource对象
 * @author sharajava
 */
public class DataSourceManager extends AbstractResourceManager implements Initialize {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceManager.class);

    /**
     * 这个接口抽象出 RM 的处理逻辑
     */
    private ResourceManagerInbound asyncWorker;

    /**
     * key: resourceId  value: dataSourceProxy
     * 看来一个RM 下可能有多个 datasource
     */
    private Map<String, Resource> dataSourceCache = new ConcurrentHashMap<>();

    /**
     * Sets async worker.
     *
     * @param asyncWorker the async worker
     */
    public void setAsyncWorker(ResourceManagerInbound asyncWorker) {
        this.asyncWorker = asyncWorker;
    }

    /**
     * 查询对应的锁语句是否已经生成  对应的应用场景为 全局事务中某个单一事务使用 for update 查询 这里不仅要实现本地事务的 读已提交 还要在全局事务范围内保证读已提交
     * 手段就是 往TC 上发送一个加锁语句 这样不同的全局事务之间就能做到事务隔离了
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
        try {
            // 创建一个查询全局锁的请求对象
            GlobalLockQueryRequest request = new GlobalLockQueryRequest();
            request.setXid(xid);
            request.setLockKey(lockKeys);
            request.setResourceId(resourceId);

            GlobalLockQueryResponse response = null;
            // 判断当前上下文 是否设置了xid
            if (RootContext.inGlobalTransaction()) {
                // 同步获取查询锁的结果  这里也是选择同一事务组下的某个address (那么 TC 之间是如何做数据同步的???)
                response = (GlobalLockQueryResponse)RmRpcClient.getInstance().sendMsgWithResponse(request);
            // 如果在本线程中绑定了某个标识(意味着需要获取全局事务锁)
            } else if (RootContext.requireGlobalLock()) {
                response = (GlobalLockQueryResponse)RmRpcClient.getInstance().sendMsgWithResponse(loadBalance(),
                    request, NettyClientConfig.getRpcRequestTimeout());
            } else {
                // 其余场景不允许出现
                throw new RuntimeException("unknow situation!");
            }

            // 通信异常
            if (response.getResultCode() == ResultCode.Failed) {
                throw new TransactionException(response.getTransactionExceptionCode(),
                    "Response[" + response.getMsg() + "]");
            }
            // 判断是否上锁成功
            return response.isLockable();
        } catch (TimeoutException toe) {
            throw new RmTransactionException(TransactionExceptionCode.IO, "RPC Timeout", toe);
        } catch (RuntimeException rex) {
            throw new RmTransactionException(TransactionExceptionCode.LockableCheckFailed, "Runtime", rex);
        }

    }

    /**
     * 通过事务Group 获取到一组合适的 serverAddress 地址 并通过均衡负载的方式获取到合适地址
     * @return
     */
    @SuppressWarnings("unchecked")
    private String loadBalance() {
        InetSocketAddress address = null;
        try {
            List<InetSocketAddress> inetSocketAddressList = RegistryFactory.getInstance().lookup(
                TmRpcClient.getInstance().getTransactionServiceGroup());
            address = LoadBalanceFactory.getInstance().select(inetSocketAddressList);
        } catch (Exception ignore) {
            LOGGER.error(ignore.getMessage());
        }
        if (address == null) {
            throw new FrameworkException(NoAvailableService);
        }
        return NetUtil.toStringAddress(address);
    }

    /**
     * Init.
     * 设置指定的异步worker 对象
     * @param asyncWorker the async worker
     */
    public synchronized void initAsyncWorker(ResourceManagerInbound asyncWorker) {
        setAsyncWorker(asyncWorker);
    }

    /**
     * Instantiates a new Data source manager.
     */
    public DataSourceManager() {
    }

    /**
     * 初始化 当该对象通过SPI 机制进行加载时会触发该方法
     */
    @Override
    public void init() {
        // AsyncWorker 用于处理 AT 模式下 提交成功的任务 (删除undo 日志)
        AsyncWorker asyncWorker = new AsyncWorker();
        // 进行初始化 开启一个定期 删除undo日志的定时器
        asyncWorker.init();
        // 就是setAsyncWorker
        initAsyncWorker(asyncWorker);
    }

    /**
     * RM 上报某个资源 到TC 上
     * @param resource
     */
    @Override
    public void registerResource(Resource resource) {
        // dataSource 被看作是 一个Resource 对象
        DataSourceProxy dataSourceProxy = (DataSourceProxy)resource;
        // 缓存资源
        dataSourceCache.put(dataSourceProxy.getResourceId(), dataSourceProxy);
        // 将资源注册到RM 上   注意这里是注册到所有 server 上 就是为了保证 (最终一致性)
        super.registerResource(dataSourceProxy);
    }

    @Override
    public void unregisterResource(Resource resource) {
        throw new NotSupportYetException("unregister a resource");
    }

    /**
     * Get data source proxy.
     * 从缓存对象中 获取某个 数据源
     * @param resourceId the resource id
     * @return the data source proxy
     */
    public DataSourceProxy get(String resourceId) {
        return (DataSourceProxy)dataSourceCache.get(resourceId);
    }

    /**
     * 提交任务 委托给 Worker 对象
     * @param branchType      the branch type
     * @param xid             Transaction id.
     * @param branchId        Branch id.
     * @param resourceId      Resource id.
     * @param applicationData Application data bind with this branch.
     * @return
     * @throws TransactionException
     */
    @Override
    public BranchStatus branchCommit(BranchType branchType, String xid, long branchId, String resourceId,
                                     String applicationData) throws TransactionException {
        return asyncWorker.branchCommit(branchType, xid, branchId, resourceId, applicationData);
    }

    /**
     * 基于 Undo日志的 RM 本地事务回滚（也就是AT 模式） 如果是 TCC 应该会调用指定的回滚方法
     * @param branchType      the branch type
     * @param xid             Transaction id.
     * @param branchId        Branch id.
     * @param resourceId      Resource id.
     * @param applicationData Application data bind with this branch.
     * @return
     * @throws TransactionException
     */
    @Override
    public BranchStatus branchRollback(BranchType branchType, String xid, long branchId, String resourceId,
                                       String applicationData) throws TransactionException {
        // 资源id 对应 dataSource
        DataSourceProxy dataSourceProxy = get(resourceId);
        if (dataSourceProxy == null) {
            throw new ShouldNeverHappenException();
        }
        try {
            // 获取日志 对象 并执行回滚
            UndoLogManagerFactory.getUndoLogManager(dataSourceProxy.getDbType()).undo(dataSourceProxy, xid, branchId);
            // 可能会抛出需要重试的异常
        } catch (TransactionException te) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("branchRollback failed reason [{}]", te.getMessage());
            }
            if (te.getCode() == TransactionExceptionCode.BranchRollbackFailed_Unretriable) {
                return BranchStatus.PhaseTwo_RollbackFailed_Unretryable;
            } else {
                return BranchStatus.PhaseTwo_RollbackFailed_Retryable;
            }
        }
        return BranchStatus.PhaseTwo_Rollbacked;

    }

    @Override
    public Map<String, Resource> getManagedResources() {
        return dataSourceCache;
    }

    @Override
    public BranchType getBranchType() {
        return BranchType.AT;
    }

}
