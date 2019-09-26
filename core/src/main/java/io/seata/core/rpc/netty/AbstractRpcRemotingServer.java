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

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.seata.common.XID;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.core.rpc.RemotingServer;
import io.seata.core.rpc.netty.v1.ProtocolV1Decoder;
import io.seata.core.rpc.netty.v1.ProtocolV1Encoder;
import io.seata.discovery.registry.RegistryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Rpc remoting server.
 * 服务端
 *
 * @author jimin.jm @alibaba-inc.com
 * @author xingfudeshi@gmail.com
 * @date 2018 /9/12
 */
public abstract class AbstractRpcRemotingServer extends AbstractRpcRemoting implements RemotingServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRpcRemotingServer.class);
    /**
     * 对应 netty 的 ServerBootStrap
     */
    private final ServerBootstrap serverBootstrap;
    /**
     * work线程组
     */
    private final EventLoopGroup eventLoopGroupWorker;
    /**
     * boss线程组
     */
    private final EventLoopGroup eventLoopGroupBoss;
    /**
     * 服务端配置
     */
    private final NettyServerConfig nettyServerConfig;
    /**
     * 监听端口
     */
    private int listenPort;
    /**
     * 确保只启动一次
     */
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * Sets listen port.
     *
     * @param listenPort the listen port
     */
    public void setListenPort(int listenPort) {

        if (listenPort <= 0) {
            throw new IllegalArgumentException("listen port: " + listenPort + " is invalid!");
        }
        this.listenPort = listenPort;
    }

    /**
     * Gets listen port.
     *
     * @return the listen port
     */
    public int getListenPort() {
        return listenPort;
    }

    /**
     * Instantiates a new Rpc remoting server.
     *
     * @param nettyServerConfig the netty server config
     */
    public AbstractRpcRemotingServer(final NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }

    /**
     * Instantiates a new Rpc remoting server.
     *
     * @param nettyServerConfig the netty server config
     * @param messageExecutor   the message executor
     * @param handlers          the handlers
     */
    public AbstractRpcRemotingServer(final NettyServerConfig nettyServerConfig,
                                     final ThreadPoolExecutor messageExecutor, final ChannelHandler... handlers) {
        // 设置消息线程池
        super(messageExecutor);
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        // 是否支持 Epoll 支持的话 使用性能更高的 eventLoopGroup
        if (NettyServerConfig.enableEpoll()) {
            this.eventLoopGroupBoss = new EpollEventLoopGroup(nettyServerConfig.getBossThreadSize(),
                    new NamedThreadFactory(nettyServerConfig.getBossThreadPrefix(), nettyServerConfig.getBossThreadSize()));
            this.eventLoopGroupWorker = new EpollEventLoopGroup(nettyServerConfig.getServerWorkerThreads(),
                    new NamedThreadFactory(nettyServerConfig.getWorkerThreadPrefix(),
                            nettyServerConfig.getServerWorkerThreads()));
        } else {
            this.eventLoopGroupBoss = new NioEventLoopGroup(nettyServerConfig.getBossThreadSize(),
                    new NamedThreadFactory(nettyServerConfig.getBossThreadPrefix(), nettyServerConfig.getBossThreadSize()));
            this.eventLoopGroupWorker = new NioEventLoopGroup(nettyServerConfig.getServerWorkerThreads(),
                    new NamedThreadFactory(nettyServerConfig.getWorkerThreadPrefix(),
                            nettyServerConfig.getServerWorkerThreads()));
        }
        // 从外部设置 handler 对象
        if (null != handlers) {
            channelHandlers = handlers;
        }
        // init listenPort in constructor so that getListenPort() will always get the exact port
        // 设置监听端口
        setListenPort(nettyServerConfig.getDefaultListenPort());
    }

    /**
     * 启动客户端
     */
    @Override
    public void start() {
        this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupWorker)
                // 这个channel 竟然有可能不是 NIO 的 不过一般还是使用 NioServerSocketChannel
                .channel(nettyServerConfig.SERVER_CHANNEL_CLAZZ)
                .option(ChannelOption.SO_BACKLOG, nettyServerConfig.getSoBackLogSize())
                .option(ChannelOption.SO_REUSEADDR, true)
                // channel 存活时间
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                // 设置发送缓冲区大小
                .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSendBufSize())
                // 设置接受缓冲区大小
                .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketResvBufSize())
                // 写水位 到一定时间内 超过该水位就会变得不可写
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                        // 设置低水位和高水位
                        new WriteBufferWaterMark(nettyServerConfig.getWriteBufferLowWaterMark(),
                                nettyServerConfig.getWriteBufferHighWaterMark()))
                // 设置本地地址
                .localAddress(new InetSocketAddress(listenPort))
                // 设置 handler
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        // 设置空闲检测对象 注意只设置了 readIdle 之后增加2个编解码器
                        ch.pipeline().addLast(new IdleStateHandler(nettyServerConfig.getChannelMaxReadIdleSeconds(), 0, 0))
                                .addLast(new ProtocolV1Decoder())
                                .addLast(new ProtocolV1Encoder());
                        if (null != channelHandlers) {
                            // 追加 handler
                            addChannelPipelineLast(ch, channelHandlers);
                        }

                    }
                });

        // 设置 bytebuf 的容量大小
        if (nettyServerConfig.isEnableServerPooledByteBufAllocator()) {
            this.serverBootstrap.childOption(ChannelOption.ALLOCATOR, NettyServerConfig.DIRECT_BYTE_BUF_ALLOCATOR);
        }

        try {
            // 阻塞直到绑定完成
            ChannelFuture future = this.serverBootstrap.bind(listenPort).sync();
            LOGGER.info("Server started ... ");
            // 将本机注册到注册中心上 这样 client 发送请求时 就可以找到该服务器
            RegistryFactory.getInstance().register(new InetSocketAddress(XID.getIpAddress(), XID.getPort()));
            // 代表初始化完成
            initialized.set(true);
            // 阻塞本线程不关闭 直到 触发 close
            future.channel().closeFuture().sync();
        } catch (Exception exx) {
            throw new RuntimeException(exx);
        }

    }

    /**
     * 关闭bootstrap
     */
    @Override
    public void shutdown() {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Shuting server down. ");
            }
            // 如果完成初始化
            if (initialized.get()) {
                // 从注册中心注销  这么看来server 是借助在注册中心上被客户端发现的
                RegistryFactory.getInstance().unregister(new InetSocketAddress(XID.getIpAddress(), XID.getPort()));
                RegistryFactory.getInstance().close();
                //wait a few seconds for server transport
                TimeUnit.SECONDS.sleep(nettyServerConfig.getServerShutdownWaitTime());
            }

            this.eventLoopGroupBoss.shutdownGracefully();
            this.eventLoopGroupWorker.shutdownGracefully();
        } catch (Exception exx) {
            LOGGER.error(exx.getMessage());
        }
    }

    /**
     * 关闭 channel
     * @param serverAddress the server address
     * @param channel       the channel
     */
    @Override
    public void destroyChannel(String serverAddress, Channel channel) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("will destroy channel:" + channel + ",address:" + serverAddress);
        }
        channel.disconnect();
        channel.close();
    }

}
