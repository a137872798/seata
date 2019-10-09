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
import io.seata.common.exception.FrameworkException;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.thread.RejectedPolicies;
import io.seata.config.Configuration;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.protocol.AbstractMessage;
import io.seata.core.protocol.RegisterTMRequest;
import io.seata.core.protocol.RegisterTMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * The type Rpc client.
 * TM 客户端对象 推测 RM 和 TM 都是 client
 * @author jimin.jm @alibaba-inc.com
 * @author zhaojun
 * @date 2018 /10/23
 */
@Sharable   // @Sharable 代表该 channelHandler 可以添加到多个pipeline 中 注意client 对象的最上层就是一个 channelHandler 对象
public final class TmRpcClient extends AbstractRpcRemotingClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(TmRpcClient.class);
    /**
     * 单例模式  代表一个应用只需要 开启一个事务client 就可以
     */
    private static volatile TmRpcClient instance;
    private static final Configuration CONFIG = ConfigurationFactory.getInstance();
    private static final long KEEP_ALIVE_TIME = Integer.MAX_VALUE;
    private static final int MAX_QUEUE_SIZE = 2000;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private String applicationId;
    private String transactionServiceGroup;
    
    /**
     * The constant enableDegrade.
     */
    public static boolean enableDegrade = false;

    /**
     * 根据config 对象初始化 TM client
     * @param nettyClientConfig
     * @param eventExecutorGroup
     * @param messageExecutor
     */
    private TmRpcClient(NettyClientConfig nettyClientConfig,
                        EventExecutorGroup eventExecutorGroup,
                        ThreadPoolExecutor messageExecutor) {
        super(nettyClientConfig, eventExecutorGroup, messageExecutor, NettyPoolKey.TransactionRole.TMROLE);
    }

    /**
     * Gets instance.
     *
     * @param applicationId           the application id
     * @param transactionServiceGroup the transaction service group
     * @return the instance
     */
    public static TmRpcClient getInstance(String applicationId, String transactionServiceGroup) {
        TmRpcClient tmRpcClient = getInstance();
        tmRpcClient.setApplicationId(applicationId);
        tmRpcClient.setTransactionServiceGroup(transactionServiceGroup);
        return tmRpcClient;
    }

    /**
     * Gets instance.
     * 初始化 TM client 对象
     * @return the instance
     */
    public static TmRpcClient getInstance() {
        if (null == instance) {
            synchronized (TmRpcClient.class) {
                if (null == instance) {
                    // 生成通信相关的配置对象
                    NettyClientConfig nettyClientConfig = new NettyClientConfig();
                    // 生成一个线程池对象   该线程池是用来处理消息的
                    final ThreadPoolExecutor messageExecutor = new ThreadPoolExecutor(
                        nettyClientConfig.getClientWorkerThreads(), nettyClientConfig.getClientWorkerThreads(),
                        KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(MAX_QUEUE_SIZE),
                        new NamedThreadFactory(nettyClientConfig.getTmDispatchThreadPrefix(),
                            nettyClientConfig.getClientWorkerThreads()),
                        RejectedPolicies.runsOldestTaskPolicy());
                    instance = new TmRpcClient(nettyClientConfig, null, messageExecutor);
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
     * 初始化 这里只是做了并发控制
     */
    @Override
    public void init() {
        if (initialized.compareAndSet(false, true)) {
            // 这里会触发静态对象的初始化   enableDegrade 代表能否降级   至于配置是怎么获取的 先不细看
            enableDegrade = CONFIG.getBoolean(ConfigurationKeys.SERVICE_PREFIX + ConfigurationKeys.ENABLE_DEGRADE_POSTFIX);
            super.init();
        }
    }

    /**
     * 原来每次销毁后 会清除实例对象
     */
    @Override
    public void destroy() {
        super.destroy();
        initialized.getAndSet(false);
        instance = null;
    }

    /**
     * 返回一个 函数 该函数可以将 address 转变成一个 poolKey 对象
     * @return
     */
    @Override
    protected Function<String, NettyPoolKey> getPoolKeyFunction() {
        return (severAddress) -> {
            // 生成注册 TM 的消息  看来每个 TM Client 对象在创建时 会注册到server(TC) 上
            RegisterTMRequest message = new RegisterTMRequest(applicationId, transactionServiceGroup);
            return new NettyPoolKey(NettyPoolKey.TransactionRole.TMROLE, severAddress, message);
        };
    }

    /**
     * 获取事务组 就是直接将初始化时设置的 事务组返回
     * @return
     */
    @Override
    public String getTransactionServiceGroup() {
        return transactionServiceGroup;
    }

    /**
     * 当注册成功时触发
     * 因为 TM 不向 RM 同时要上报 resource信息 所以直接缓存 channel就好
     * @param serverAddress  the server address
     * @param channel        the channel
     * @param response       the response 代表响应消息
     * @param requestMessage the request message
     */
    @Override
    public void onRegisterMsgSuccess(String serverAddress, Channel channel, Object response,
                                     AbstractMessage requestMessage) {
        // 将服务端channel 通过manager维护
        getClientChannelManager().registerChannel(serverAddress, channel);
    }

    /**
     * 注册失败时触发  直接抛出异常
     * @param serverAddress  the server address
     * @param channel        the channel
     * @param response       the response
     * @param requestMessage the request message
     */
    @Override
    public void onRegisterMsgFail(String serverAddress, Channel channel, Object response,
                                  AbstractMessage requestMessage) {
        if (response instanceof RegisterTMResponse && LOGGER.isInfoEnabled()) {
            LOGGER.info("register client failed, server version:"
                + ((RegisterTMResponse)response).getVersion());
        }
        throw new FrameworkException("register client app failed.");
    }
}
