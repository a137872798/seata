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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.seata.common.util.NetUtil;
import io.seata.core.protocol.HeartbeatMessage;
import io.seata.core.protocol.RegisterRMRequest;
import io.seata.core.protocol.RegisterTMRequest;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.rpc.ChannelManager;
import io.seata.core.rpc.DefaultServerMessageListenerImpl;
import io.seata.core.rpc.RpcContext;
import io.seata.core.rpc.ServerMessageListener;
import io.seata.core.rpc.ServerMessageSender;
import io.seata.core.rpc.TransactionMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

/**
 * The type Abstract rpc server.
 * 通过Rpc访问的服务端
 * @author jimin.jm @alibaba-inc.com
 * @date 2018 /10/15
 */
@Sharable
public class RpcServer extends AbstractRpcRemotingServer implements ServerMessageSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(RpcServer.class);

    /**
     * The Server message listener.
     * 处理服务端接收到的消息  默认实现就是将注册消息对应的数据保存到容器中  事务消息则是委托给TransactionMessageListener
     */
    protected ServerMessageListener serverMessageListener;

    /**
     * 处理事务相关的消息
     */
    private TransactionMessageHandler transactionMessageHandler;
    /**
     * 注册权限校验  对用户开放的钩子
     */
    private RegisterCheckAuthHandler checkAuthHandler;

    /**
     * Sets transactionMessageHandler.
     *
     * @param transactionMessageHandler the transactionMessageHandler
     */
    public void setHandler(TransactionMessageHandler transactionMessageHandler) {
        setHandler(transactionMessageHandler, null);
    }

    /**
     * Sets transactionMessageHandler.
     *
     * @param transactionMessageHandler the transactionMessageHandler
     * @param checkAuthHandler          the check auth handler
     */
    public void setHandler(TransactionMessageHandler transactionMessageHandler,
                           RegisterCheckAuthHandler checkAuthHandler) {
        this.transactionMessageHandler = transactionMessageHandler;
        this.checkAuthHandler = checkAuthHandler;
    }

    /**
     * Instantiates a new Abstract rpc server.
     * 通过一个处理消息的线程池进行初始化
     * @param messageExecutor the message executor
     */
    public RpcServer(ThreadPoolExecutor messageExecutor) {
        super(new NettyServerConfig(), messageExecutor);
    }

    /**
     * Gets server message listener.
     *
     * @return the server message listener
     */
    public ServerMessageListener getServerMessageListener() {
        return serverMessageListener;
    }

    /**
     * Sets server message listener.
     *
     * @param serverMessageListener the server message listener
     */
    public void setServerMessageListener(ServerMessageListener serverMessageListener) {
        this.serverMessageListener = serverMessageListener;
    }

    /**
     * Debug log.
     *
     * @param info the info
     */
    public void debugLog(String info) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(info);
        }
    }

    /**
     * Init.
     * 初始化 服务端
     */
    @Override
    public void init() {
        super.init();
        // 将本对象追加到 ServerBootStrap中
        setChannelHandlers(RpcServer.this);
        // 使用默认的消息监听器实现
        DefaultServerMessageListenerImpl defaultServerMessageListenerImpl = new DefaultServerMessageListenerImpl(
            transactionMessageHandler);
        defaultServerMessageListenerImpl.init();
        defaultServerMessageListenerImpl.setServerMessageSender(this);
        this.setServerMessageListener(defaultServerMessageListenerImpl);
        // 启动 serverBootStrap
        super.start();
    }

    private void closeChannelHandlerContext(ChannelHandlerContext ctx) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("closeChannelHandlerContext channel:" + ctx.channel());
        }
        // 关闭连接 触发 close 函数
        ctx.disconnect();
        ctx.close();
    }

    /**
     * User event triggered.
     * 空闲检测对象
     * @param ctx the ctx
     * @param evt the evt
     * @throws Exception the exception
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            debugLog("idle:" + evt);
            IdleStateEvent idleStateEvent = (IdleStateEvent)evt;
            if (idleStateEvent.state() == IdleState.READER_IDLE) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("channel:" + ctx.channel() + " read idle.");
                }
                handleDisconnect(ctx);
                try {
                    closeChannelHandlerContext(ctx);
                } catch (Exception e) {
                    LOGGER.error(e.getMessage());
                }
            }
        }
    }

    /**
     * Destroy.
     */
    @Override
    public void destroy() {
        super.destroy();
        super.shutdown();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("destroyed rpcServer");
        }
    }

    /**
     * Send response.
     * rm reg,rpc reg,inner response
     * 返回响应信息
     * @param request the request
     * @param channel the channel
     * @param msg     the msg
     */
    @Override
    public void sendResponse(RpcMessage request, Channel channel, Object msg) {
        Channel clientChannel = channel;
        // 非心跳消息时
        if (!(msg instanceof HeartbeatMessage)) {
            // 从 channelManager 中找到对应的客户端channel
            clientChannel = ChannelManager.getSameClientChannel(channel);
        }
        if (clientChannel != null) {
            // 通过上层处理
            super.sendResponse(request, clientChannel, msg);
        } else {
            throw new RuntimeException("channel is error. channel:" + clientChannel);
        }
    }

    /**
     * Send request with response object.
     * send syn request for rm
     * 同步响应消息给RM
     * @param resourceId the db key
     * @param clientId   the client ip
     * @param message    the message
     * @param timeout    the timeout
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    @Override
    public Object sendSyncRequest(String resourceId, String clientId, Object message,
                                  long timeout) throws TimeoutException {
        // 这里一定要匹配到 某个channel 即使 不同ip 不同应用 但是这样有什么意义吗  响应结果返回给不同的client 能确保功能正常执行吗
        Channel clientChannel = ChannelManager.getChannel(resourceId, clientId);
        if (clientChannel == null) {
            throw new RuntimeException("rm client is not connected. dbkey:" + resourceId
                + ",clientId:" + clientId);

        }
        // 阻塞给定时间
        return sendAsyncRequestWithResponse(null, clientChannel, message, timeout);
    }

    /**
     * Send request with response object.
     * 使用默认的 超时时间 阻塞发送消息
     * @param resourceId the db key
     * @param clientId   the client ip
     * @param message    the msg
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    @Override
    public Object sendSyncRequest(String resourceId, String clientId, Object message)
        throws TimeoutException {
        return sendSyncRequest(resourceId, clientId, message, NettyServerConfig.getRpcRequestTimeout());
    }

    /**
     * Send request with response object.
     * 异步发送消息  也就是不设置超时时间
     * @param channel   the channel
     * @param message    the msg
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    @Override
    public Object sendASyncRequest(Channel channel, Object message) throws IOException, TimeoutException {
       return sendAsyncRequestWithoutResponse(channel, message);
    }

    /**
     * Dispatch.
     * 上层收到消息后会通过 dispatch 来分发并处理消息
     * @param request the request
     * @param ctx   the ctx
     */
    @Override
    public void dispatch(RpcMessage request, ChannelHandlerContext ctx) {
        Object msg = request.getBody();
        // 代表注册 RM 消息 就是保存到一个容器中
        if (msg instanceof RegisterRMRequest) {
            serverMessageListener.onRegRmMessage(request, ctx, this,
                checkAuthHandler);
        } else {
            // 代表是事务消息 事务消息的话首先要确保当前channel 已经注册
            if (ChannelManager.isRegistered(ctx.channel())) {
                // 处理事务相关的消息 比如 全局事务注册  分事务提交 分事务回滚
                serverMessageListener.onTrxMessage(request, ctx, this);
            } else {
                try {
                    // channel 还没有注册 这里关闭channel  且没有返回任何结果 那么对端就会自动超时
                    closeChannelHandlerContext(ctx);
                } catch (Exception exx) {
                    LOGGER.error(exx.getMessage());
                }
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(String.format("close a unhandled connection! [%s]", ctx.channel().toString()));
                }
            }
        }
    }

    /**
     * Channel inactive.
     * 当channel 失活时触发
     * @param ctx the ctx
     * @throws Exception the exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        debugLog("inactive:" + ctx);
        if (messageExecutor.isShutdown()) {
            return;
        }
        // 处理断开连接的逻辑  当server 关闭时也会触发
        handleDisconnect(ctx);
        super.channelInactive(ctx);
    }

    /**
     * 断开连接
     * @param ctx
     */
    private void handleDisconnect(ChannelHandlerContext ctx) {
        final String ipAndPort = NetUtil.toStringAddress(ctx.channel().remoteAddress());
        RpcContext rpcContext = ChannelManager.getContextFromIdentified(ctx.channel());
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(ipAndPort + " to server channel inactive.");
        }
        // 做一些清理工作
        if (null != rpcContext && null != rpcContext.getClientRole()) {
            rpcContext.release();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("remove channel:" + ctx.channel() + "context:" + rpcContext);
            }
        } else {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("remove unused channel:" + ctx.channel());
            }
        }
    }

    /**
     * Channel read.
     * TC 端处理收到的请求 比如 RM 注册 TM 开启全局事务
     * @param ctx the ctx
     * @param msg the msg
     * @throws Exception the exception
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
        // 代表是 Rpc 消息
        if (msg instanceof RpcMessage) {
            RpcMessage rpcMessage = (RpcMessage) msg;
            debugLog("read:" + rpcMessage.getBody());
            // 每个client 启动时 会通过TM 向 同一事务组的 TC 发送注册请求 也就是RegisterTMRequest
            if (rpcMessage.getBody() instanceof RegisterTMRequest) {
                serverMessageListener.onRegTmMessage(rpcMessage, ctx, this, checkAuthHandler);
                return;
            }
            // 在 seata 中 心跳是由 client 向服务器发出 服务器返回PONG
            if (rpcMessage.getBody() == HeartbeatMessage.PING) {
                serverMessageListener.onCheckMessage(rpcMessage, ctx, this);
                return;
            }
        }
        super.channelRead(ctx, msg);
    }

    /**
     * Exception caught.
     * 当捕获到异常是 打印日志 并释放channel
     * @param ctx   the ctx
     * @param cause the cause
     * @throws Exception the exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("channel exx:" + cause.getMessage() + ",channel:" + ctx.channel());
        }
        ChannelManager.releaseRpcContext(ctx.channel());
        super.exceptionCaught(ctx, cause);
    }
}
