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
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.seata.common.exception.FrameworkErrorCode;
import io.seata.common.exception.FrameworkException;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.thread.PositiveAtomicCounter;
import io.seata.core.protocol.HeartbeatMessage;
import io.seata.core.protocol.MergeMessage;
import io.seata.core.protocol.MessageFuture;
import io.seata.core.protocol.ProtocolConstants;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.rpc.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The type Abstract rpc remoting.
 * Client/Server 的基类  继承双端handler
 * @author jimin.jm @alibaba-inc.com
 * @date 2018 /9/12
 */
public abstract class AbstractRpcRemoting extends ChannelDuplexHandler implements Disposable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRpcRemoting.class);
    /**
     * The Timer executor.
     * 超时检测 应该是记录从 数据流入 和流出的耗时时间
     */
    protected final ScheduledExecutorService timerExecutor = new ScheduledThreadPoolExecutor(1,
        new NamedThreadFactory("timeoutChecker", 1, true));
    /**
     * The Message executor.
     * 使用额外的线程池避免阻塞 netty的独占线程
     */
    protected final ThreadPoolExecutor messageExecutor;

    /**
     * Id generator of this remoting
     * 一个保证正数的 计数器
     */
    protected final PositiveAtomicCounter idGenerator = new PositiveAtomicCounter();

    /**
     * The Futures.
     * 存放一组 future 对象
     */
    protected final ConcurrentHashMap<Integer, MessageFuture> futures = new ConcurrentHashMap<>();
    /**
     * The Basket map.
     * key: address value: 应该是对应要发送到该地址的rpc消息
     */
    protected final ConcurrentHashMap<String, BlockingQueue<RpcMessage>> basketMap = new ConcurrentHashMap<>();

    private static final long NOT_WRITEABLE_CHECK_MILLS = 10L;
    /**
     * The Merge lock.
     * 整合消息时的 对象锁
     */
    protected final Object mergeLock = new Object();
    /**
     * The Now mills.
     * 当前时间戳 会在每次触发定时任务时更新
     */
    protected volatile long nowMills = 0;
    /**
     * 超时检测时间间隔
     */
    private static final int TIMEOUT_CHECK_INTERNAL = 3000;
    /**
     * 锁对象
     */
    private final Object lock = new Object();
    /**
     * The Is sending.
     * 判断是否已经发送
     */
    protected volatile boolean isSending = false;
    private String group = "DEFAULT";
    /**
     * The Merge msg map.
     * 存放merge 消息的容器
     */
    protected final Map<Integer, MergeMessage> mergeMsgMap = new ConcurrentHashMap<>();
    /**
     * The Channel handlers.
     * 该对象维护了一组channelHandler
     */
    protected ChannelHandler[] channelHandlers;

    /**
     * Instantiates a new Abstract rpc remoting.
     * 传入一个用于处理消息的线程池对象
     * @param messageExecutor the message executor
     */
    public AbstractRpcRemoting(ThreadPoolExecutor messageExecutor) {
        this.messageExecutor = messageExecutor;
    }

    /**
     * Gets next message id.
     *
     * @return the next message id
     */
    public int getNextMessageId() {
        return idGenerator.incrementAndGet();
    }

    /**
     * Init.
     * 初始化方法 启动定时器
     */
    public void init() {
        timerExecutor.scheduleAtFixedRate(new Runnable() {

            /**
             * 定时清理 未产生结果的MessageFuture对象
             */
            @Override
            public void run() {
                List<MessageFuture> timeoutMessageFutures = new ArrayList<MessageFuture>(futures.size());
                for (MessageFuture future : futures.values()) {
                    // 判断任务是否超时
                    if (future.isTimeout()) {
                        // 添加到超时队列中
                        timeoutMessageFutures.add(future);
                    }
                }
                for (MessageFuture messageFuture : timeoutMessageFutures) {
                    // 从当前容器中移除 超时future 对象
                    futures.remove(messageFuture.getRequestMessage().getId());
                    // 通过设置结果的方式 强制性唤醒等待线程  这里的设计跟rocketMQ 很像
                    messageFuture.setResultMessage(null);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("timeout clear future : " + messageFuture.getRequestMessage().getBody());
                    }
                }
                // 同时更新 nowMills
                nowMills = System.currentTimeMillis();
            }
        }, TIMEOUT_CHECK_INTERNAL, TIMEOUT_CHECK_INTERNAL, TimeUnit.MILLISECONDS);
    }

    /**
     * Destroy.
     * 关闭线程池 实际上线程池不再接受新的任务 并且处理已经存放于队列中的任务
     */
    @Override
    public void destroy() {
        timerExecutor.shutdown();
        messageExecutor.shutdown();
    }

    /**
     * 当channel 可写入状态发生变化时 触发 这个函数有点忘了  注意寻找 配对的 synchronized (lock) 位置
     * @param ctx
     */
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        synchronized (lock) {
            // 如果变成了可写状态 唤醒阻塞的其他lock
            // 当尝试发送消息时 如果当前channel 不可写会进入阻塞状态 等待这里唤醒它们
            if (ctx.channel().isWritable()) {
                lock.notifyAll();
            }
        }

        // 向下传播事件
        ctx.fireChannelWritabilityChanged();
    }

    /**
     * Send async request with response object.
     * 发送异步请求 会抛出超时异常
     *
     * @param channel the channel
     * @param msg     the msg
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    protected Object sendAsyncRequestWithResponse(Channel channel, Object msg) throws TimeoutException {
        return sendAsyncRequestWithResponse(null, channel, msg, NettyClientConfig.getRpcRequestTimeout());
    }

    /**
     * Send async request with response object.
     *
     * @param address the address
     * @param channel the channel
     * @param msg     the msg
     * @param timeout the timeout
     * @return the object
     * @throws TimeoutException the timeout exception
     * 发送异步请求 并返回结果
     */
    protected Object sendAsyncRequestWithResponse(String address, Channel channel, Object msg, long timeout) throws
        TimeoutException {
        if (timeout <= 0) {
            throw new FrameworkException("timeout should more than 0ms");
        }
        return sendAsyncRequest(address, channel, msg, timeout);
    }

    /**
     * Send async request without response object.
     * 发送异步请求
     * @param channel the channel
     * @param msg     the msg
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    protected Object sendAsyncRequestWithoutResponse(Channel channel, Object msg) throws
        TimeoutException {
        return sendAsyncRequest(null, channel, msg, 0);
    }

    /**
     * 发送异步请求
     * @param address
     * @param channel
     * @param msg
     * @param timeout
     * @return
     * @throws TimeoutException
     */
    private Object sendAsyncRequest(String address, Channel channel, Object msg, long timeout)
        throws TimeoutException {
        if (channel == null) {
            LOGGER.warn("sendAsyncRequestWithResponse nothing, caused by null channel.");
            return null;
        }
        final RpcMessage rpcMessage = new RpcMessage();
        // 获取唯一标识
        rpcMessage.setId(getNextMessageId());
        // 默认设置 oneway 类型 代表不需要返回数据
        rpcMessage.setMessageType(ProtocolConstants.MSGTYPE_RESQUEST_ONEWAY);
        // 指定序列化方式
        rpcMessage.setCodec(ProtocolConstants.CONFIGURED_CODEC);
        // 指定是否压缩
        rpcMessage.setCompressor(ProtocolConstants.CONFIGURED_COMPRESSOR);
        // 将对象作为数据实体
        rpcMessage.setBody(msg);

        // 将本发送对象设置到 futures 中
        final MessageFuture messageFuture = new MessageFuture();
        messageFuture.setRequestMessage(rpcMessage);
        messageFuture.setTimeout(timeout);
        futures.put(rpcMessage.getId(), messageFuture);

        // 如果指定了地址 就代表经过均衡负载后指定了某个TC 节点
        if (address != null) {
            ConcurrentHashMap<String, BlockingQueue<RpcMessage>> map = basketMap;
            BlockingQueue<RpcMessage> basket = map.get(address);
            // 未创建情况下 进行初始化
            if (basket == null) {
                map.putIfAbsent(address, new LinkedBlockingQueue<>());
                basket = map.get(address);
            }
            // 将消息设置到阻塞队列中 有一个 asyncWorker 对象会获取basketMap的数据并发送
            basket.offer(rpcMessage);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("offer message: " + rpcMessage.getBody());
            }
            // 未发送情况下 唤醒发送
            if (!isSending) {
                synchronized (mergeLock) {
                    mergeLock.notifyAll();
                }
            }
        } else {
            // 未指定地址的情况下走这里
            // 发送注册消息时不会指定地址 也就是将信息注册到同一事务组下所有 TC 节点
            ChannelFuture future;
            // 先检测 channel 是否可写入  如果不可写入 会阻塞线程 直到被唤醒(也就是变成可写 或者 超过最大重试次数)
            channelWriteableCheck(channel, msg);
            // 将数据写入到channel中  之后就变成异步操作了 所以发送动作是异步的
            future = channel.writeAndFlush(rpcMessage);
            // 增加监听器
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    // 当失败的情况下
                    if (!future.isSuccess()) {
                        // 移除掉对应的消息
                        MessageFuture messageFuture = futures.remove(rpcMessage.getId());
                        if (messageFuture != null) {
                            // 同时唤醒阻塞的线程 (调用get()的线程)
                            messageFuture.setResultMessage(future.cause());
                        }
                        // 失败情况销毁channel
                        destroyChannel(future.channel());
                    }
                }
            });
        }

        // 如果存在超时时间 阻塞本线程 当收到 res 消息后 也会唤醒线程
        if (timeout > 0) {
            try {
                return messageFuture.get(timeout, TimeUnit.MILLISECONDS);
            } catch (Exception exx) {
                LOGGER.error("wait response error:" + exx.getMessage() + ",ip:" + address + ",request:" + msg);
                if (exx instanceof TimeoutException) {
                    throw (TimeoutException)exx;
                } else {
                    throw new RuntimeException(exx);
                }
            }
        // 没有超时时间 就不进行阻塞
        } else {
            return null;
        }
    }

    /**
     * Send request.
     * 同步发送请求消息
     * @param channel the channel  用于发送消息的channel remote 是server
     * @param msg     the msg  发送的消息体 在Seats 中消息支持批量发送
     */
    protected void sendRequest(Channel channel, Object msg) {
        RpcMessage rpcMessage = new RpcMessage();
        // 如果是心跳消息 设置对应的消息类型  否则就是普通的请求消息
        rpcMessage.setMessageType(msg instanceof HeartbeatMessage ?
                ProtocolConstants.MSGTYPE_HEARTBEAT_REQUEST
                : ProtocolConstants.MSGTYPE_RESQUEST);
        // 设置指定的序列化工具
        rpcMessage.setCodec(ProtocolConstants.CONFIGURED_CODEC);
        // 是否压缩
        rpcMessage.setCompressor(ProtocolConstants.CONFIGURED_COMPRESSOR);
        // 数据体
        rpcMessage.setBody(msg);
        // id 生成器 为每条消息自动设置id
        rpcMessage.setId(getNextMessageId());
        // 如果待发送的消息是 merge 消息  存放到 merge 容器中
        if (msg instanceof MergeMessage) {
            mergeMsgMap.put(rpcMessage.getId(), (MergeMessage)msg);
        }
        // 检测可否写入 不可写入就阻塞  不能写入应该就是底层缓冲区满了 需要 选择器匹配到 写操作
        channelWriteableCheck(channel, msg);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("write message:" + rpcMessage.getBody() + ", channel:" + channel + ",active?"
                + channel.isActive() + ",writable?" + channel.isWritable() + ",isopen?" + channel.isOpen());
        }
        // 写入消息  这里写入也会直接返回啊没有体现出 同步的动作
        channel.writeAndFlush(rpcMessage);
    }

    /**
     * Send response.
     * 发送结果  该方法对应client 的处理请求 返回响应结果
     * @param request  the msg id
     * @param channel the channel
     * @param msg     the msg
     */
    protected void sendResponse(RpcMessage request, Channel channel, Object msg) {
        RpcMessage rpcMessage = new RpcMessage();
        rpcMessage.setMessageType(msg instanceof HeartbeatMessage ?
                ProtocolConstants.MSGTYPE_HEARTBEAT_RESPONSE :
                ProtocolConstants.MSGTYPE_RESPONSE);
        rpcMessage.setCodec(request.getCodec()); // same with request
        rpcMessage.setCompressor(request.getCompressor());
        rpcMessage.setBody(msg);
        rpcMessage.setId(request.getId());
        channelWriteableCheck(channel, msg);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("send response:" + rpcMessage.getBody() + ",channel:" + channel);
        }
        channel.writeAndFlush(rpcMessage);
    }

    /**
     * 检测 channel 是否可写入数据
     * @param channel
     * @param msg
     */
    private void channelWriteableCheck(Channel channel, Object msg) {
        int tryTimes = 0;
        synchronized (lock) {
            while (!channel.isWritable()) {
                try {
                    tryTimes++;
                    // 当不能写入时 有一个最大的重试次数 超过的情况下 关闭该channel
                    if (tryTimes > NettyClientConfig.getMaxNotWriteableRetry()) {
                        destroyChannel(channel);
                        throw new FrameworkException("msg:" + ((msg == null) ? "null" : msg.toString()),
                            FrameworkErrorCode.ChannelIsNotWritable);
                    }
                    // 该线程沉睡 等待 channel 变成可写的
                    lock.wait(NOT_WRITEABLE_CHECK_MILLS);
                } catch (InterruptedException exx) {
                    LOGGER.error(exx.getMessage());
                }
            }
        }
    }

    /**
     * For testing. When the thread pool is full, you can change this variable and share the stack
     */
    boolean allowDumpStack = false;

    /**
     * 当client/server 接受到 对端传来的消息时 根据消息类型进行分发
     * @param ctx 当前对应的 channel
     * @param msg 传入的请求数据体
     * @throws Exception
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
        // 如果接收到的是 RpcMessage   好像TC 主动发送的消息是不需要去 futures 中移除对应数据的  只有当client 自身将数据发往server时因为需要等待结果所以 使用了响应池
        if (msg instanceof RpcMessage) {
            final RpcMessage rpcMessage = (RpcMessage)msg;
            // 收到请求消息
            if (rpcMessage.getMessageType() == ProtocolConstants.MSGTYPE_RESQUEST
                    || rpcMessage.getMessageType() == ProtocolConstants.MSGTYPE_RESQUEST_ONEWAY) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("%s msgId:%s, body:%s", this, rpcMessage.getId(), rpcMessage.getBody()));
                }
                try {
                    // 使用线程池去分发处理
                    AbstractRpcRemoting.this.messageExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                dispatch(rpcMessage, ctx);
                            } catch (Throwable th) {
                                LOGGER.error(FrameworkErrorCode.NetDispatch.getErrCode(), th.getMessage(), th);
                            }
                        }
                    });
                    // 如果消息满了  测试用这里不看
                } catch (RejectedExecutionException e) {
                    LOGGER.error(FrameworkErrorCode.ThreadPoolFull.getErrCode(),
                        "thread pool is full, current max pool size is " + messageExecutor.getActiveCount());
                    if (allowDumpStack) {
                        String name = ManagementFactory.getRuntimeMXBean().getName();
                        String pid = name.split("@")[0];
                        int idx = new Random().nextInt(100);
                        try {
                            Runtime.getRuntime().exec("jstack " + pid + " >d:/" + idx + ".log");
                        } catch (IOException exx) {
                            LOGGER.error(exx.getMessage());
                        }
                        allowDumpStack = false;
                    }
                }
                // 代表接受到响应消息 那么 对应的 消息池就可以移除掉
            } else {
                MessageFuture messageFuture = futures.remove(rpcMessage.getId());
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String
                        .format("%s msgId:%s, future :%s, body:%s", this, rpcMessage.getId(), messageFuture,
                            rpcMessage.getBody()));
                }
                // 设置结果 唤醒阻塞线程
                if (messageFuture != null) {
                    messageFuture.setResultMessage(rpcMessage.getBody());
                } else {
                    try {
                        AbstractRpcRemoting.this.messageExecutor.execute(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    dispatch(rpcMessage, ctx);
                                } catch (Throwable th) {
                                    LOGGER.error(FrameworkErrorCode.NetDispatch.getErrCode(), th.getMessage(), th);
                                }
                            }
                        });
                    } catch (RejectedExecutionException e) {
                        LOGGER.error(FrameworkErrorCode.ThreadPoolFull.getErrCode(),
                            "thread pool is full, current max pool size is " + messageExecutor.getActiveCount());
                    }
                }
            }
        }
    }

    /**
     * 当捕获到异常时 该方法属于 Netty对外开放的钩子
     * @param ctx
     * @param cause
     * @throws Exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOGGER.error(FrameworkErrorCode.ExceptionCaught.getErrCode(),
            ctx.channel() + " connect exception. " + cause.getMessage(),
            cause);
        try {
            // 捕获到异常时关闭channel
            destroyChannel(ctx.channel());
        } catch (Exception e) {
            LOGGER.error("failed to close channel {}: {}", ctx.channel(), e.getMessage(), e);
        }
    }

    /**
     * Dispatch.
     * 处理接受到的消息 子类实现
     * @param request the request
     * @param ctx     the ctx
     */
    public abstract void dispatch(RpcMessage request, ChannelHandlerContext ctx);

    /**
     * 打印日志
     * @param ctx
     * @param future
     * @throws Exception
     */
    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(ctx + " will closed");
        }
        super.close(ctx, future);
    }

    /**
     * Add channel pipeline last.
     * 追加一些handlers 到 channel 上  应该是对用户开放的
     * @param channel  the channel
     * @param handlers the handlers
     */
    protected void addChannelPipelineLast(Channel channel, ChannelHandler... handlers) {
        if (null != channel && null != handlers) {
            channel.pipeline().addLast(handlers);
        }
    }

    /**
     * Sets channel handlers.
     *
     * @param handlers the handlers
     */
    protected void setChannelHandlers(ChannelHandler... handlers) {
        this.channelHandlers = handlers;
    }

    /**
     * Gets group.
     * 这里的组是以什么为单位划分的
     * @return the group
     */
    public String getGroup() {
        return group;
    }

    /**
     * Sets group.
     *
     * @param group the group
     */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * Destroy channel.
     * 销毁某个channel
     * @param channel the channel
     */
    public void destroyChannel(Channel channel) {
        destroyChannel(getAddressFromChannel(channel), channel);
    }

    /**
     * Destroy channel.
     * 销毁channel 由子类实现
     * @param serverAddress the server address
     * @param channel       the channel
     */
    public abstract void destroyChannel(String serverAddress, Channel channel);

    /**
     * Gets address from context.
     *
     * @param ctx the ctx
     * @return the address from context
     */
    protected String getAddressFromContext(ChannelHandlerContext ctx) {
        return getAddressFromChannel(ctx.channel());
    }

    /**
     * Gets address from channel.
     * 从channel 上获取对端地址
     * @param channel the channel
     * @return the address from channel
     */
    protected String getAddressFromChannel(Channel channel) {
        SocketAddress socketAddress = channel.remoteAddress();
        String address = socketAddress.toString();
        if (socketAddress.toString().indexOf(NettyClientConfig.getSocketAddressStartChar()) == 0) {
            address = socketAddress.toString().substring(NettyClientConfig.getSocketAddressStartChar().length());
        }
        return address;
    }
}
