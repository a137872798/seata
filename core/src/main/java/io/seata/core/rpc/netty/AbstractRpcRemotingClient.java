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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.EventExecutorGroup;
import io.seata.common.exception.FrameworkErrorCode;
import io.seata.common.exception.FrameworkException;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.NetUtil;
import io.seata.core.protocol.AbstractMessage;
import io.seata.core.protocol.HeartbeatMessage;
import io.seata.core.protocol.MergeResultMessage;
import io.seata.core.protocol.MergedWarpMessage;
import io.seata.core.protocol.MessageFuture;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.rpc.ClientMessageListener;
import io.seata.core.rpc.ClientMessageSender;
import io.seata.discovery.loadbalance.LoadBalanceFactory;
import io.seata.discovery.registry.RegistryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.seata.common.exception.FrameworkErrorCode.NoAvailableService;

/**
 * The type Rpc remoting client.
 * TM client和 RM client的骨架类
 * @author jimin.jm @alibaba-inc.com
 * @author zhaojun
 * @date 2018 /9/12
 */
public abstract class AbstractRpcRemotingClient extends AbstractRpcRemoting
    implements RegisterMsgListener, ClientMessageSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRpcRemotingClient.class);
    private static final String MSG_ID_PREFIX = "msgId:";
    private static final String FUTURES_PREFIX = "futures:";
    private static final String SINGLE_LOG_POSTFIX = ";";
    private static final int MAX_MERGE_SEND_MILLS = 1;
    private static final String THREAD_PREFIX_SPLIT_CHAR = "_";
    
    private static final int MAX_MERGE_SEND_THREAD = 1;
    private static final long KEEP_ALIVE_TIME = Integer.MAX_VALUE;
    /**
     * 定时器扫描间隔
     */
    private static final int SCHEDULE_INTERVAL_MILLS = 5;
    private static final String MERGE_THREAD_PREFIX = "rpcMergeMessageSend";

    /**
     * 客户端引导对象 用于连接到 server
     */
    private final RpcClientBootstrap clientBootstrap;
    /**
     * 管理 该client 连接的所有server
     */
    private NettyClientChannelManager clientChannelManager;
    /**
     * 客户端消息监听器
     */
    private ClientMessageListener clientMessageListener;
    /**
     * 角色信息 有 RM TM SERVER
     */
    private final NettyPoolKey.TransactionRole transactionRole;
    /**
     * 用于merge 消息的线程池对象
     */
    private ExecutorService mergeSendExecutorService;

    /**
     *
     * @param nettyClientConfig  通信相关配置类
     * @param eventExecutorGroup  通过单例模式构造下该值为null
     * @param messageExecutor  代表消息相关的线程池
     * @param transactionRole  代表本client 的角色  TM/RM
     */
    public AbstractRpcRemotingClient(NettyClientConfig nettyClientConfig, EventExecutorGroup eventExecutorGroup,
                                     ThreadPoolExecutor messageExecutor, NettyPoolKey.TransactionRole transactionRole) {
        super(messageExecutor);
        this.transactionRole = transactionRole;
        // 使用给定的config 去初始化 bootstrap 对象  注意本对象也是一个 channelHandler 对象 (因为继承自 ChannelDuplexHandler)
        // 这里没有启动 bootstrap
        clientBootstrap = new RpcClientBootstrap(nettyClientConfig, eventExecutorGroup, this, transactionRole);
        // 生成channel 管理对象 应该是使用一个 bootstrap 对象维护多个连接
        clientChannelManager = new NettyClientChannelManager(
                // 使用一个池化工厂
            new NettyPoolableFactory(this, clientBootstrap), getPoolKeyFunction(), nettyClientConfig);
    }
    
    public NettyClientChannelManager getClientChannelManager() {
        return clientChannelManager;
    }
    
    /**
     * Get pool key function.
     * 获取生成 pool 键的方法
     * @return lambda function
     */
    protected abstract Function<String, NettyPoolKey> getPoolKeyFunction();
    
    /**
     * Get transaction service group.
     * 获取事务组
     * @return transaction service group
     */
    protected abstract String getTransactionServiceGroup();

    @Override
    public void init() {
        // 这里是设置 netty.bootstrap 的相关配置 而没有执行connect 方法
        clientBootstrap.start();
        // 启动定时器
        timerExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // 委托给 ChannelManager 进行定期重连   参数是事务组 (该属性在初始化 Scanner bean 对象时设置)
                // 这里一旦发现是活的channel 就会进行重连
                clientChannelManager.reconnect(getTransactionServiceGroup());
            }
        }, SCHEDULE_INTERVAL_MILLS, SCHEDULE_INTERVAL_MILLS, TimeUnit.SECONDS);
        // 开启一个 merge 消息的线程池对象
        mergeSendExecutorService = new ThreadPoolExecutor(MAX_MERGE_SEND_THREAD,
            MAX_MERGE_SEND_THREAD,
            KEEP_ALIVE_TIME, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new NamedThreadFactory(getThreadPrefix(), MAX_MERGE_SEND_THREAD));
        // 启动任务  将要发送的消息 整合后发送 而非直接发送
        mergeSendExecutorService.submit(new MergedSendRunnable());
        // 开启定时器扫描future 中没有结果的对象 并关闭
        super.init();
    }

    /**
     * 关闭连接 以及定时器
     */
    @Override
    public void destroy() {
        clientBootstrap.shutdown();
        mergeSendExecutorService.shutdown();
    }

    /**
     * 当接受到读事件时触发
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof RpcMessage)) {
            return;
        }
        RpcMessage rpcMessage = (RpcMessage) msg;
        // 如果收到心跳消息 在这层进行拦截 记得 dubbo 是使用了多层去拦截 有层是专门用于处理HeartBeat 的
        // 这里要注意下 如果没有收到心跳是如何处理的
        if (rpcMessage.getBody() == HeartbeatMessage.PONG) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("received PONG from {}", ctx.channel().remoteAddress());
            }
            return;
        }
        // 如果是整合的消息
        if (rpcMessage.getBody() instanceof MergeResultMessage) {
            MergeResultMessage results = (MergeResultMessage) rpcMessage.getBody();
            // 从消息池中移除掉 merge 消息
            MergedWarpMessage mergeMessage = (MergedWarpMessage) mergeMsgMap.remove(rpcMessage.getId());
            for (int i = 0; i < mergeMessage.msgs.size(); i++) {
                int msgId = mergeMessage.msgIds.get(i);
                // 拆分后又从 存储单条消息的池中移除
                MessageFuture future = futures.remove(msgId);
                if (future == null) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("msg: {} is not found in futures.", msgId);
                    }
                } else {
                    // 设置结果 并唤醒阻塞的线程(调用 future.get()的线程)
                    future.setResultMessage(results.getMsgs()[i]);
                }
            }
            return;
        }
        // 上层就是分发消息 也就是 触发 dispatch方法
        super.channelRead(ctx, msg);
    }

    /**
     * 当接受到 req RpcMsg 或者 res RpcMsg 时 通过该方法做转发
     * @param request the request
     * @param ctx     the ctx
     */
    @Override
    public void dispatch(RpcMessage request, ChannelHandlerContext ctx) {
        // 如果存在监听器的情况 使用监听器去处理
        if (clientMessageListener != null) {
            String remoteAddress = NetUtil.toStringAddress(ctx.channel().remoteAddress());
            clientMessageListener.onMessage(request, remoteAddress, this);
        }
    }

    /**
     * 当channel 失活时触发
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 如果消息处理线程池已经关闭就 直接返回
        if (messageExecutor.isShutdown()) {
            return;
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("channel inactive: {}", ctx.channel());
        }
        // 释放channel
        clientChannelManager.releaseChannel(ctx.channel(), NetUtil.toStringAddress(ctx.channel().remoteAddress()));
        super.channelInactive(ctx);
    }

    /**
     * 用户自定义事件触发 该函数一般是做心跳检测用的
     * @param ctx
     * @param evt
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idleStateEvent = (IdleStateEvent)evt;
            // 如果是长期没有读取到数据
            if (idleStateEvent.state() == IdleState.READER_IDLE) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("channel" + ctx.channel() + " read idle.");
                }
                try {
                    String serverAddress = NetUtil.toStringAddress(ctx.channel().remoteAddress());
                    // 标记某个 channel 已经无效
                    clientChannelManager.invalidateObject(serverAddress, ctx.channel());
                } catch (Exception exx) {
                    LOGGER.error(exx.getMessage());
                } finally {
                    // 某条channel 脱离pool 的管理
                    clientChannelManager.releaseChannel(ctx.channel(), getAddressFromContext(ctx));
                }
            }
            // 如果长时间没有触发写操作
            if (idleStateEvent == IdleStateEvent.WRITER_IDLE_STATE_EVENT) {
                try {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("will send ping msg,channel" + ctx.channel());
                    }
                    // 发送心跳 相当于是被动发送 而不是通过某个定时任务主动去发送
                    sendRequest(ctx.channel(), HeartbeatMessage.PING);
                } catch (Throwable throwable) {
                    LOGGER.error("send request error: {}", throwable.getMessage(), throwable);
                }
            }
        }
    }

    /**
     * 捕获异常  父类只是输出一条日志
     * @param ctx
     * @param cause
     * @throws Exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOGGER.error(FrameworkErrorCode.ExceptionCaught.getErrCode(),
            NetUtil.toStringAddress(ctx.channel().remoteAddress()) + "connect exception. " + cause.getMessage(), cause);
        // 释放某条channel
        clientChannelManager.releaseChannel(ctx.channel(), getAddressFromChannel(ctx.channel()));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("remove exception rm channel:" + ctx.channel());
        }
        super.exceptionCaught(ctx, cause);
    }

    /**
     * 将消息发送到 事务组中的某个 TC TODO 这里要注意 没有实现一致性 那么TC 节点间会在什么时候做同步呢???
     * @param msg     the msg
     * @param timeout the timeout
     * @return
     * @throws TimeoutException
     */
    @Override
    public Object sendMsgWithResponse(Object msg, long timeout) throws TimeoutException {
        // 获取 事务组 并使用均衡负载寻找合适的 address
        // 这里会维护 同一事务组下所有server 的连接
        String validAddress = loadBalance(getTransactionServiceGroup());
        // 通过 address 寻找channel 对象 没有的话应该会创建一条新的
        Channel channel = clientChannelManager.acquireChannel(validAddress);
        // 存在超时时间 会阻塞直到 接受到 res  或者超时
        Object result = super.sendAsyncRequestWithResponse(validAddress, channel, msg, timeout);
        return result;
    }

    /**
     * 发送数据 使用默认的超时时间
     * @param msg the msg
     * @return
     * @throws TimeoutException
     */
    @Override
    public Object sendMsgWithResponse(Object msg) throws TimeoutException {
        return sendMsgWithResponse(msg, NettyClientConfig.getRpcRequestTimeout());
    }

    /**
     * 使用直连方式访问
     * @param serverAddress the server address
     * @param msg           the msg
     * @param timeout       the timeout
     * @return
     * @throws TimeoutException
     */
    @Override
    public Object sendMsgWithResponse(String serverAddress, Object msg, long timeout)
        throws TimeoutException {
        return sendAsyncRequestWithResponse(serverAddress, clientChannelManager.acquireChannel(serverAddress), msg, timeout);
    }

    /**
     * 发送响应结果
     * @param request       the msg id
     * @param serverAddress the server address
     * @param msg           the msg
     */
    @Override
    public void sendResponse(RpcMessage request, String serverAddress, Object msg) {
        super.sendResponse(request, clientChannelManager.acquireChannel(serverAddress), msg);
    }
    
    /**
     * Gets client message listener.
     *
     * @return the client message listener
     */
    public ClientMessageListener getClientMessageListener() {
        return clientMessageListener;
    }
    
    /**
     * Sets client message listener.
     * 设置消息监听器 用于处理消息
     * @param clientMessageListener the client message listener
     */
    public void setClientMessageListener(ClientMessageListener clientMessageListener) {
        this.clientMessageListener = clientMessageListener;
    }

    /**
     * 通过channelManager 对象统一管理 channel 这里是关闭某个channel
     * @param serverAddress the server address
     * @param channel       the channel
     */
    @Override
    public void destroyChannel(String serverAddress, Channel channel) {
        clientChannelManager.destroyChannel(serverAddress, channel);
    }

    /**
     * 根据 事务组 通过均衡负载找到合适的 单台对象 这里肯定是通过注册中心动态获取地址
     * @param transactionServiceGroup
     * @return
     */
    private String loadBalance(String transactionServiceGroup) {
        InetSocketAddress address = null;
        try {
            // 使用注册中心配合 均衡负载 获取地址
            List<InetSocketAddress> inetSocketAddressList = RegistryFactory.getInstance().lookup(transactionServiceGroup);
            address = LoadBalanceFactory.getInstance().select(inetSocketAddressList);
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
        if (address == null) {
            throw new FrameworkException(NoAvailableService);
        }
        return NetUtil.toStringAddress(address);
    }
    
    private String getThreadPrefix() {
        return AbstractRpcRemotingClient.MERGE_THREAD_PREFIX + THREAD_PREFIX_SPLIT_CHAR + transactionRole.name();
    }

    /**
     * The type Merged send runnable.
     * 用于merge 消息的 任务 由一个executor来执行
     */
    private class MergedSendRunnable implements Runnable {

        @Override
        public void run() {
            // 保证任务不退出
            while (true) {
                synchronized (mergeLock) {
                    try {
                        // 等待唤醒  当发送某个消息后 会唤醒该线程
                        // 就会立即发送消息
                        mergeLock.wait(MAX_MERGE_SEND_MILLS);
                    } catch (InterruptedException e) {
                    }
                }
                // 代表正在发送中
                isSending = true;
                // 开始遍历桶中的数据  要发往TC 的数据会先暂存到basketMap 中等待批量发送
                for (String address : basketMap.keySet()) {
                    // 每个地址有对应的待发送的消息
                    BlockingQueue<RpcMessage> basket = basketMap.get(address);
                    if (basket.isEmpty()) {
                        continue;
                    }

                    // 将对应地址中所有消息 整合成一条 并发送
                    MergedWarpMessage mergeMessage = new MergedWarpMessage();
                    while (!basket.isEmpty()) {
                        // 不断从阻塞队列中拉取消息 并整合到 mergeMsg 中
                        RpcMessage msg = basket.poll();
                        mergeMessage.msgs.add((AbstractMessage) msg.getBody());
                        mergeMessage.msgIds.add(msg.getId());
                    }
                    // 打印消息
                    if (mergeMessage.msgIds.size() > 1) {
                        printMergeMessageLog(mergeMessage);
                    }
                    Channel sendChannel = null;
                    try {
                        // 获取地址并发送消息  这里没有阻塞
                        sendChannel = clientChannelManager.acquireChannel(address);
                        // 将消息通过channel 发送到对应address 的服务器
                        sendRequest(sendChannel, mergeMessage);
                    } catch (FrameworkException e) {
                        // 如果是通道无法写入 销毁该channel
                        if (e.getErrcode() == FrameworkErrorCode.ChannelIsNotWritable && sendChannel != null) {
                            destroyChannel(address, sendChannel);
                        }
                        // fast fail
                        for (Integer msgId : mergeMessage.msgIds) {
                            // future 代表等待 接受结果的 future 对象 在通信模型中就是一个响应池
                            MessageFuture messageFuture = futures.remove(msgId);
                            if (messageFuture != null) {
                                // 设置响应结果为null 没有使用重发机制吗???
                                messageFuture.setResultMessage(null);
                            }
                        }
                        LOGGER.error("client merge call failed: {}", e.getMessage(), e);
                    }
                }
                // 看来正在发送中已经是会阻止某些操作
                isSending = false;
            }
        }

        private void printMergeMessageLog(MergedWarpMessage mergeMessage) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("merge msg size:" + mergeMessage.msgIds.size());
                for (AbstractMessage cm : mergeMessage.msgs) {
                    LOGGER.debug(cm.toString());
                }
                StringBuilder sb = new StringBuilder();
                for (long l : mergeMessage.msgIds) {
                    sb.append(MSG_ID_PREFIX).append(l).append(SINGLE_LOG_POSTFIX);
                }
                sb.append("\n");
                for (long l : futures.keySet()) {
                    sb.append(FUTURES_PREFIX).append(l).append(SINGLE_LOG_POSTFIX);
                }
                LOGGER.debug(sb.toString());
            }
        }
    }
}
