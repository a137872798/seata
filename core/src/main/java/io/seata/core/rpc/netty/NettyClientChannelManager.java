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
import io.seata.common.exception.FrameworkErrorCode;
import io.seata.common.exception.FrameworkException;
import io.seata.common.util.CollectionUtils;
import io.seata.common.util.NetUtil;
import io.seata.core.protocol.RegisterRMRequest;
import io.seata.discovery.registry.RegistryFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Netty client pool manager.
 * 客户端连接管理
 * @author jimin.jm @alibaba-inc.com
 * @author zhaojun
 */
class NettyClientChannelManager {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClientChannelManager.class);

    // 下面代表以多种方式进行映射的容器

    /**
     * key: address value: 对应的 锁对象
     */
    private final ConcurrentMap<String, Object> channelLocks = new ConcurrentHashMap<>();

    /**
     * key: address value: 事务角色信息
     */
    private final ConcurrentMap<String, NettyPoolKey> poolKeyMap = new ConcurrentHashMap<>();

    /**
     * address -> channel
     */
    private final ConcurrentMap<String, Channel> channels = new ConcurrentHashMap<>();

    /**
     * 事务信息 对应channel 先不细看 推测是一个map
     */
    private final GenericKeyedObjectPool<NettyPoolKey, Channel> nettyClientKeyPool;

    /**
     * 通过地址 生成 poolKey 的函数
     */
    private Function<String, NettyPoolKey> poolKeyFunction;

    /**
     * 初始化
     * @param keyPoolableFactory 该对象维护了 channel的创建和关闭 等
     * @param poolKeyFunction
     * @param clientConfig
     */
    NettyClientChannelManager(final NettyPoolableFactory keyPoolableFactory, final Function<String, NettyPoolKey> poolKeyFunction,
                                     final NettyClientConfig clientConfig) {
        // 使用池化工厂进行初始化
        nettyClientKeyPool = new GenericKeyedObjectPool<>(keyPoolableFactory);
        nettyClientKeyPool.setConfig(getNettyPoolConfig(clientConfig));
        // 使用服务端地址 创建poolKey 的函数
        this.poolKeyFunction = poolKeyFunction;
    }
    
    private GenericKeyedObjectPool.Config getNettyPoolConfig(final NettyClientConfig clientConfig) {
        GenericKeyedObjectPool.Config poolConfig = new GenericKeyedObjectPool.Config();
        poolConfig.maxActive = clientConfig.getMaxPoolActive();
        poolConfig.minIdle = clientConfig.getMinPoolIdle();
        poolConfig.maxWait = clientConfig.getMaxAcquireConnMills();
        poolConfig.testOnBorrow = clientConfig.isPoolTestBorrow();
        poolConfig.testOnReturn = clientConfig.isPoolTestReturn();
        poolConfig.lifo = clientConfig.isPoolLifo();
        return poolConfig;
    }
    
    /**
     * Get all channels registered on current Rpc Client.
     *
     * @return channels
     */
    ConcurrentMap<String, Channel> getChannels() {
        return channels;
    }
    
    /**
     * Acquire netty client channel connected to remote server.
     * 尝试获取 channel
     * @param serverAddress server address
     * @return netty channel
     */
    Channel acquireChannel(String serverAddress) {
        // 首先从 池中获取
        Channel channelToServer = channels.get(serverAddress);
        if (channelToServer != null) {
            // 确保channel 存活
            channelToServer = getExistAliveChannel(channelToServer, serverAddress);
            if (null != channelToServer) {
                return channelToServer;
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("will connect to " + serverAddress);
        }
        // 如果channel 不存在 就创建一个新连接
        channelLocks.putIfAbsent(serverAddress, new Object());
        synchronized (channelLocks.get(serverAddress)) {
            return doConnect(serverAddress);
        }
    }
    
    /**
     * Release channel to pool if necessary.
     * 归还某个 channel 到 pool中
     * @param channel channel
     * @param serverAddress server address
     */
    void releaseChannel(Channel channel, String serverAddress) {
        if (null == channel || null == serverAddress) { return; }
        try {
            synchronized (channelLocks.get(serverAddress)) {
                // 首先确保没有连接了 才进行归还
                Channel ch = channels.get(serverAddress);
                if (null == ch) {
                    nettyClientKeyPool.returnObject(poolKeyMap.get(serverAddress), channel);
                    return;
                }
                if (ch.compareTo(channel) == 0) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("return to pool, rm channel:" + channel);
                    }
                    // 这里头也会调用 returnObject 不过还会同时从 channels 中移除该channel
                    destroyChannel(serverAddress, channel);
                } else {
                    nettyClientKeyPool.returnObject(poolKeyMap.get(serverAddress), channel);
                }
            }
        } catch (Exception exx) {
            LOGGER.error(exx.getMessage());
        }
    }
    
    /**
     * Destroy channel.
     * 销毁某个channel
     * @param serverAddress server address
     * @param channel channel
     */
    void destroyChannel(String serverAddress, Channel channel) {
        if (null == channel) { return; }
        try {
            if (channel.equals(channels.get(serverAddress))) {
                channels.remove(serverAddress);
            }
            nettyClientKeyPool.returnObject(poolKeyMap.get(serverAddress), channel);
        } catch (Exception exx) {
            LOGGER.error("return channel to rmPool error:" + exx.getMessage());
        }
    }
    
    /**
     * Reconnect to remote server of current transaction service group.
     * 重新连接到某个 transactionGroup
     * @param transactionServiceGroup transaction service group
     */
    void reconnect(String transactionServiceGroup) {
        List<String> availList = null;
        try {
            // 通过注册中心寻找候选list
            availList = getAvailServerList(transactionServiceGroup);
        } catch (Exception exx) {
            LOGGER.error("Failed to get available servers: {}", exx.getMessage());
        }
        if (CollectionUtils.isEmpty(availList)) {
            LOGGER.error("no available server to connect.");
            return;
        }
        for (String serverAddress : availList) {
            try {
                // 获取每个 server 对应的channel 如果不存在就新建一个 同时每个channel 在创建后自动会发送 注册消息
                acquireChannel(serverAddress);
            } catch (Exception e) {
                LOGGER.error(FrameworkErrorCode.NetConnect.getErrCode(),
                    "can not connect to " + serverAddress + " cause:" + e.getMessage(), e);
            }
        }
    }

    /**
     * 标记某个 channel 已经无效了
     * @param serverAddress
     * @param channel
     * @throws Exception
     */
    void invalidateObject(final String serverAddress, final Channel channel) throws Exception {
        nettyClientKeyPool.invalidateObject(poolKeyMap.get(serverAddress), channel);
    }

    /**
     * 注册某个channel
     * @param serverAddress
     * @param channel
     */
    void registerChannel(final String serverAddress, final Channel channel) {
        if (null != channels.get(serverAddress) && channels.get(serverAddress).isActive()) {
            return;
        }
        channels.put(serverAddress, channel);
    }

    /**
     * 创建一个新的channel
     * @param serverAddress
     * @return
     */
    private Channel doConnect(String serverAddress) {
        Channel channelToServer = channels.get(serverAddress);
        if (channelToServer != null && channelToServer.isActive()) {
            return channelToServer;
        }
        Channel channelFromPool;
        try {
            // 使用函数对象 生成key  该key 中会携带 TMRegisterMsg 或者 RMRegisterMsg 这样就在获取channel的同时完成注册
            NettyPoolKey currentPoolKey = poolKeyFunction.apply(serverAddress);
            NettyPoolKey previousPoolKey = poolKeyMap.putIfAbsent(serverAddress, currentPoolKey);
            // 如果已经存在key了
            if (null != previousPoolKey && previousPoolKey.getMessage() instanceof RegisterRMRequest) {
                RegisterRMRequest registerRMRequest = (RegisterRMRequest) currentPoolKey.getMessage();
                // 使用新的 resourceId
                ((RegisterRMRequest) previousPoolKey.getMessage()).setResourceIds(registerRMRequest.getResourceIds());
            }
            // 从池中获取一个channel
            channelFromPool = nettyClientKeyPool.borrowObject(poolKeyMap.get(serverAddress));
            channels.put(serverAddress, channelFromPool);
        } catch (Exception exx) {
            LOGGER.error(FrameworkErrorCode.RegisterRM.getErrCode(), "register RM failed.", exx);
            throw new FrameworkException("can not register RM,err:" + exx.getMessage());
        }
        return channelFromPool;
    }

    /**
     * 通过注册中心 寻找 server列表
     * @param transactionServiceGroup
     * @return
     * @throws Exception
     */
    private List<String> getAvailServerList(String transactionServiceGroup) throws Exception {
        List<String> availList = new ArrayList<>();
        List<InetSocketAddress> availInetSocketAddressList = RegistryFactory.getInstance().lookup(
            transactionServiceGroup);
        if (!CollectionUtils.isEmpty(availInetSocketAddressList)) {
            for (InetSocketAddress address : availInetSocketAddressList) {
                availList.add(NetUtil.toStringAddress(address));
            }
        }
        return availList;
    }

    /**
     * 确保channel 是否存活 不存活的话返回null
     * @param rmChannel
     * @param serverAddress
     * @return
     */
    private Channel getExistAliveChannel(Channel rmChannel, String serverAddress) {
        if (rmChannel.isActive()) {
            return rmChannel;
        } else {
            int i = 0;
            // 检测重试次数
            for (; i < NettyClientConfig.getMaxCheckAliveRetry(); i++) {
                try {
                    Thread.sleep(NettyClientConfig.getCheckAliveInternal());
                } catch (InterruptedException exx) {
                    LOGGER.error(exx.getMessage());
                }
                // 也许每隔一段时间 就会有channel 生成
                rmChannel = channels.get(serverAddress);
                if (null != rmChannel && rmChannel.isActive()) {
                    return rmChannel;
                }
            }
            if (i == NettyClientConfig.getMaxCheckAliveRetry()) {
                LOGGER.warn("channel " + rmChannel + " is not active after long wait, close it.");
                releaseChannel(rmChannel, serverAddress);
                return null;
            }
        }
        return null;
    }
}

