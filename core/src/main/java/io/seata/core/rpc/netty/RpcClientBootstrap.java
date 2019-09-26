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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.internal.PlatformDependent;
import io.seata.common.exception.FrameworkException;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.core.rpc.RemotingClient;
import io.seata.core.rpc.netty.v1.ProtocolV1Decoder;
import io.seata.core.rpc.netty.v1.ProtocolV1Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Rpc client.
 * 客户端引导程序
 *
 * @author jimin.jm @alibaba-inc.com
 * @author zhaojun
 */
public class RpcClientBootstrap implements RemotingClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRpcRemotingClient.class);
    private final NettyClientConfig nettyClientConfig;
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    /**
     * 应该是 将channel 中耗时的操作都委托到该对象上
     */
    private EventExecutorGroup defaultEventExecutorGroup;
    /**
     * 就是一个 管理 channel 的容器
     */
    private AbstractChannelPoolMap<InetSocketAddress, FixedChannelPool> clientChannelPool;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private static final String THREAD_PREFIX_SPLIT_CHAR = "_";
    private final ChannelHandler channelHandler;
    private final NettyPoolKey.TransactionRole transactionRole;

    /**
     * 初始化 Client 对象
     *
     * @param nettyClientConfig
     * @param eventExecutorGroup
     * @param channelHandler
     * @param transactionRole
     */
    public RpcClientBootstrap(NettyClientConfig nettyClientConfig, final EventExecutorGroup eventExecutorGroup,
                              ChannelHandler channelHandler, NettyPoolKey.TransactionRole transactionRole) {
        if (null == nettyClientConfig) {
            nettyClientConfig = new NettyClientConfig();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("use default netty client config.");
            }
        }
        this.nettyClientConfig = nettyClientConfig;
        int selectorThreadSizeThreadSize = this.nettyClientConfig.getClientSelectorThreadSize();
        this.transactionRole = transactionRole;
        // 使用参数 设置 EventLoopGroup
        this.eventLoopGroupWorker = new NioEventLoopGroup(selectorThreadSizeThreadSize,
                new NamedThreadFactory(getThreadPrefix(this.nettyClientConfig.getClientSelectorThreadPrefix()),
                        selectorThreadSizeThreadSize));
        this.defaultEventExecutorGroup = eventExecutorGroup;
        this.channelHandler = channelHandler;
    }

    /**
     * 启动client 对象
     */
    @Override
    public void start() {
        if (this.defaultEventExecutorGroup == null) {
            this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyClientConfig.getClientWorkerThreads(),
                    new NamedThreadFactory(getThreadPrefix(nettyClientConfig.getClientWorkerThreadPrefix()),
                            nettyClientConfig.getClientWorkerThreads()));
        }
        this.bootstrap.group(this.eventLoopGroupWorker).channel(
                nettyClientConfig.getClientChannelClazz()).option(
                ChannelOption.TCP_NODELAY, true).option(ChannelOption.SO_KEEPALIVE, true).option(
                ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis()).option(
                ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize()).option(ChannelOption.SO_RCVBUF,
                nettyClientConfig.getClientSocketRcvBufSize());

        if (nettyClientConfig.enableNative()) {
            if (PlatformDependent.isOsx()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("client run on macOS");
                }
            } else {
                bootstrap.option(EpollChannelOption.EPOLL_MODE, EpollMode.EDGE_TRIGGERED)
                        .option(EpollChannelOption.TCP_QUICKACK, true);
            }
        }
        // 判断是否使用连接池  连接池是什么???
        if (nettyClientConfig.isUseConnPool()) {
            clientChannelPool = new AbstractChannelPoolMap<InetSocketAddress, FixedChannelPool>() {
                @Override
                protected FixedChannelPool newPool(InetSocketAddress key) {
                    return new FixedChannelPool(
                            // 池中每个channel 共用一个 bootstrap 该对象本身封装了建立连接的逻辑
                            bootstrap.remoteAddress(key),
                            /**
                             * 默认的pool 处理器 包含几个生命周期对应的钩子
                             */
                            new DefaultChannelPoolHandler() {
                                /**
                                 * 当某个channel 被触发时
                                 * @param ch
                                 * @throws Exception
                                 */
                                @Override
                                public void channelCreated(Channel ch) throws Exception {
                                    super.channelCreated(ch);
                                    // 获取对应的 管道对象
                                    final ChannelPipeline pipeline = ch.pipeline();
                                    // 为每个新建的channel 对象追加 handler  注意这里指定了该handler 处理任务时使用的 线程池
                                    pipeline.addLast(defaultEventExecutorGroup,
                                            // 增加idle 检测对象
                                            new IdleStateHandler(nettyClientConfig.getChannelMaxReadIdleSeconds(),
                                                    nettyClientConfig.getChannelMaxWriteIdleSeconds(),
                                                    nettyClientConfig.getChannelMaxAllIdleSeconds()));
                                    // 追加client 处理器
                                    pipeline.addLast(defaultEventExecutorGroup, new RpcClientHandler());
                                }
                            },
                            ChannelHealthChecker.ACTIVE,
                            FixedChannelPool.AcquireTimeoutAction.FAIL,
                            nettyClientConfig.getMaxAcquireConnMills(),
                            nettyClientConfig.getPerHostMaxConn(),
                            nettyClientConfig.getPendingConnSize(),
                            false
                    );
                }
            };
        } else {
            // 非池化连接 指定handler 对象
            bootstrap.handler(
                    new ChannelInitializer<SocketChannel>() {

                        @Override
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(
                                    new IdleStateHandler(nettyClientConfig.getChannelMaxReadIdleSeconds(),
                                            nettyClientConfig.getChannelMaxWriteIdleSeconds(),
                                            nettyClientConfig.getChannelMaxAllIdleSeconds()))
                                    // 池化的不用加编解码器吗 ???
                                    .addLast(new ProtocolV1Decoder())
                                    .addLast(new ProtocolV1Encoder());
                            if (null != channelHandler) {
                                ch.pipeline().addLast(channelHandler);
                            }
                        }
                    });
        }
        if (initialized.compareAndSet(false, true) && LOGGER.isInfoEnabled()) {
            LOGGER.info("RpcClientBootstrap has started");
        }
    }

    /**
     * 关闭池对象 关闭线程池
     */
    @Override
    public void shutdown() {
        try {
            if (null != clientChannelPool) {
                clientChannelPool.close();
            }
            this.eventLoopGroupWorker.shutdownGracefully();
            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception exx) {
            LOGGER.error("Failed to shutdown: {}", exx.getMessage());
        }
    }

    /**
     * Gets new channel.
     * 创建一条新的channel 对象
     * @param address the address
     * @return the new channel
     */
    public Channel getNewChannel(InetSocketAddress address) {
        Channel channel;
        // 连接到指定地址
        ChannelFuture f = this.bootstrap.connect(address);
        try {
            // 阻塞等待连接完成
            f.await(this.nettyClientConfig.getConnectTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (f.isCancelled()) {
                throw new FrameworkException(f.cause(), "connect cancelled, can not connect to services-server.");
            } else if (!f.isSuccess()) {
                throw new FrameworkException(f.cause(), "connect failed, can not connect to services-server.");
            } else {
                channel = f.channel();
            }
        } catch (Exception e) {
            throw new FrameworkException(e, "can not connect to services-server.");
        }
        return channel;
    }

    /**
     * Gets thread prefix.
     *
     * @param threadPrefix the thread prefix
     * @return the thread prefix
     */
    private String getThreadPrefix(String threadPrefix) {
        return threadPrefix + THREAD_PREFIX_SPLIT_CHAR + transactionRole.name();
    }
}
