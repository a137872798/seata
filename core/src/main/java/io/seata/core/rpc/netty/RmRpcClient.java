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
package io.seata.core.rpc.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.util.concurrent.EventExecutorGroup;
import io.seata.common.exception.FrameworkErrorCode;
import io.seata.common.exception.FrameworkException;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.core.model.Resource;
import io.seata.core.model.ResourceManager;
import io.seata.core.protocol.AbstractMessage;
import io.seata.core.protocol.RegisterRMRequest;
import io.seata.core.protocol.RegisterRMResponse;
import io.seata.core.rpc.netty.NettyPoolKey.TransactionRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static io.seata.common.Constants.DBKEYS_SPLIT_CHAR;

/**
 * The type Rm rpc client.
 * 客户端对象 代表着 本机作为一个资源管理器 可以向 server 上报事务状态
 * @author jimin.jm @alibaba-inc.com
 * @author zhaojun
 * @date 2018 /10/10
 */
@Sharable
public final class RmRpcClient extends AbstractRpcRemotingClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RmRpcClient.class);
    /**
     * 该对象应该是接受 数据并管理
     */
    private ResourceManager resourceManager;
    private static volatile RmRpcClient instance;
    /**
     * 这个是干嘛的
     */
    private String customerKeys;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private static final long KEEP_ALIVE_TIME = Integer.MAX_VALUE;
    private static final int MAX_QUEUE_SIZE = 20000;
    private String applicationId;
    private String transactionServiceGroup;
    
    private RmRpcClient(NettyClientConfig nettyClientConfig, EventExecutorGroup eventExecutorGroup,
                        ThreadPoolExecutor messageExecutor) {
        super(nettyClientConfig, eventExecutorGroup, messageExecutor, TransactionRole.RMROLE);
    }

    /**
     * Gets instance.
     *
     * @param applicationId           the application id
     * @param transactionServiceGroup the transaction service group
     * @return the instance
     */
    public static RmRpcClient getInstance(String applicationId, String transactionServiceGroup) {
        RmRpcClient rmRpcClient = getInstance();
        rmRpcClient.setApplicationId(applicationId);
        rmRpcClient.setTransactionServiceGroup(transactionServiceGroup);
        return rmRpcClient;
    }

    /**
     * Gets instance.
     *
     * @return the instance
     */
    public static RmRpcClient getInstance() {
        if (null == instance) {
            synchronized (RmRpcClient.class) {
                if (null == instance) {
                    NettyClientConfig nettyClientConfig = new NettyClientConfig();
                    final ThreadPoolExecutor messageExecutor = new ThreadPoolExecutor(
                        nettyClientConfig.getClientWorkerThreads(), nettyClientConfig.getClientWorkerThreads(),
                        KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(MAX_QUEUE_SIZE),
                        new NamedThreadFactory(nettyClientConfig.getRmDispatchThreadPrefix(),
                            nettyClientConfig.getClientWorkerThreads()),
                        new ThreadPoolExecutor.CallerRunsPolicy());
                    instance = new RmRpcClient(nettyClientConfig, null, messageExecutor);
                }
            }
        }
        return instance;
    }
    
    /**
     * Sets application id.
     *
     * @param applicationId the application id
     */
    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }
    
    /**
     * Sets transaction service group.
     *
     * @param transactionServiceGroup the transaction service group
     */
    public void setTransactionServiceGroup(String transactionServiceGroup) {
        this.transactionServiceGroup = transactionServiceGroup;
    }
    
    /**
     * Sets resource manager.
     *
     * @param resourceManager the resource manager
     */
    public void setResourceManager(ResourceManager resourceManager) {
        this.resourceManager = resourceManager;
    }
    
    /**
     * Gets customer keys.
     *
     * @return the customer keys
     */
    public String getCustomerKeys() {
        return customerKeys;
    }
    
    /**
     * Sets customer keys.
     *
     * @param customerKeys the customer keys
     */
    public void setCustomerKeys(String customerKeys) {
        this.customerKeys = customerKeys;
    }
    
    @Override
    public void init() {
        if (initialized.compareAndSet(false, true)) {
            super.init();
        }
    }
    
    @Override
    public void destroy() {
        super.destroy();
        initialized.getAndSet(false);
        instance = null;
    }
    
    @Override
    protected Function<String, NettyPoolKey> getPoolKeyFunction() {
        return (serverAddress) -> {
            String resourceIds = customerKeys == null ? getMergedResourceKeys() : customerKeys;
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("RM will register :" + resourceIds);
            }
            RegisterRMRequest message = new RegisterRMRequest(applicationId, transactionServiceGroup);
            message.setResourceIds(resourceIds);
            return new NettyPoolKey(NettyPoolKey.TransactionRole.RMROLE, serverAddress, message);
        };
    }
    
    @Override
    protected String getTransactionServiceGroup() {
        return transactionServiceGroup;
    }

    /**
     * 当注册到 server 成功时触发
     * @param serverAddress  the server address
     * @param channel        the channel
     * @param response       the response
     * @param requestMessage the request message
     */
    @Override
    public void onRegisterMsgSuccess(String serverAddress, Channel channel, Object response,
                                     AbstractMessage requestMessage) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(
                "register RM success. server version:" + ((RegisterRMResponse)response).getVersion()
                    + ",channel:" + channel);
        }
        // 如果自定义 key 不存在
        if (customerKeys == null) {
            getClientChannelManager().registerChannel(serverAddress, channel);
            // 拼接资源信息 生成唯一标识
            String dbKey = getMergedResourceKeys();
            RegisterRMRequest message = (RegisterRMRequest)requestMessage;
            if (message.getResourceIds() != null) {
                // 资源不匹配 发送到 server 注册??? 啥意思
                if (!message.getResourceIds().equals(dbKey)) {
                    sendRegisterMessage(serverAddress, channel, dbKey);
                }
            }
        }
    }

    /**
     * 当注册消息失败时 抛出异常
     * @param serverAddress  the server address
     * @param channel        the channel
     * @param response       the response
     * @param requestMessage the request message
     */
    @Override
    public void onRegisterMsgFail(String serverAddress, Channel channel, Object response,
                                  AbstractMessage requestMessage) {

        if (response instanceof RegisterRMResponse && LOGGER.isInfoEnabled()) {
            LOGGER.info(
                "register RM failed. server version:" + ((RegisterRMResponse)response).getVersion());
        }
        throw new FrameworkException("register RM failed.");
    }
    
    /**
     * Register new db key.
     * 注册资源
     * @param resourceGroupId the resource group id  所在资源组名
     * @param resourceId      the db key  资源id
     */
    public void registerResource(String resourceGroupId, String resourceId) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("register to RM resourceId:" + resourceId);
        }
        // 尝试重新连接到 某个 server 组中 (应该是通过注册中心 + LoadBalance 进行负载之后 重连)
        if (getClientChannelManager().getChannels().isEmpty()) {
            getClientChannelManager().reconnect(transactionServiceGroup);
            return;
        }
        synchronized (getClientChannelManager().getChannels()) {
            for (Map.Entry<String, Channel> entry : getClientChannelManager().getChannels().entrySet()) {
                String serverAddress = entry.getKey();
                Channel rmChannel = entry.getValue();
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("register resource, resourceId:" + resourceId);
                }
                // 将资源发送所有server 中 保证一致性  (因为这样每个server 数据都是一致的 所以实现分布式的一致性)
                sendRegisterMessage(serverAddress, rmChannel, resourceId);
            }
        }
    }

    /**
     * 将注册请求发送到 server   这个注册是什么意思 告诉server 这里有一个 RM 管理器吗
     * 既然有了注册中心为什么RM 和 TM 不直接交由注册中心管理呢
     * @param serverAddress
     * @param channel
     * @param dbKey
     */
    private void sendRegisterMessage(String serverAddress, Channel channel, String dbKey) {
        RegisterRMRequest message = new RegisterRMRequest(applicationId, transactionServiceGroup);
        // 将资源信息设置到 message 中并进行异步发送
        message.setResourceIds(dbKey);
        try {
            super.sendAsyncRequestWithoutResponse(channel, message);
        } catch (FrameworkException e) {
            if (e.getErrcode() == FrameworkErrorCode.ChannelIsNotWritable
                && serverAddress != null) {
                getClientChannelManager().releaseChannel(channel, serverAddress);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("remove channel:" + channel);
                }
            } else {
                LOGGER.error("register failed: {}", e.getMessage(), e);
            }
        } catch (TimeoutException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * 将管理资源拼接起来形成唯一标识
     * @return
     */
    private String getMergedResourceKeys() {
        Map<String, Resource> managedResources = resourceManager.getManagedResources();
        Set<String> resourceIds = managedResources.keySet();
        if (!resourceIds.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (String resourceId : resourceIds) {
                if (first) {
                    first = false;
                } else {
                    sb.append(DBKEYS_SPLIT_CHAR);
                }
                sb.append(resourceId);
            }
            return sb.toString();
        }
        return null;
    }
}
