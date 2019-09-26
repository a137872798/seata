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
import io.seata.common.exception.FrameworkException;
import io.seata.common.util.NetUtil;
import io.seata.core.protocol.RegisterRMResponse;
import io.seata.core.protocol.RegisterTMResponse;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * The type Netty key poolable factory.
 * 池化工厂  具备创建 对象 销毁对象 激活对象 和 使失活
 * @author jimin.jm @alibaba-inc.com
 * @date 2018 /11/19
 */
public class NettyPoolableFactory implements KeyedPoolableObjectFactory<NettyPoolKey, Channel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyPoolableFactory.class);

    /**
     * client 对象
     */
    private final AbstractRpcRemotingClient rpcRemotingClient;

    /**
     * client 对应的 启动对象
     */
    private final RpcClientBootstrap clientBootstrap;

    /**
     * Instantiates a new Netty key poolable factory.
     *
     * @param rpcRemotingClient the rpc remoting client
     */
    public NettyPoolableFactory(AbstractRpcRemotingClient rpcRemotingClient,
                                RpcClientBootstrap clientBootstrap) {
        this.rpcRemotingClient = rpcRemotingClient;
        this.clientBootstrap = clientBootstrap;
    }

    /**
     * 通过 一个 标识唯一对象的poolKey 初始化 channel
     * @param key
     * @return
     */
    @Override
    public Channel makeObject(NettyPoolKey key) {
        InetSocketAddress address = NetUtil.toInetSocketAddress(key.getAddress());
            if (LOGGER.isInfoEnabled()) {
            LOGGER.info("NettyPool create channel to " + key);
        }
        // 内部就是通过 bootstrap 连接到指定地址
        Channel tmpChannel = clientBootstrap.getNewChannel(address);
        long start = System.currentTimeMillis();
        Object response;
        Channel channelToServer = null;
        if (null == key.getMessage()) {
            throw new FrameworkException(
                "register msg is null, role:" + key.getTransactionRole().name());
        }
        try {
            // 一般key上携带的就是 RM 注册消息 或者 TM 注册消息 这里就代表着 某个channel 一旦被初始化就发送一条注册消息到 server 上
            // 这里内部会使用一个超时时间去 阻塞等待结果
            response = rpcRemotingClient.sendAsyncRequestWithResponse(tmpChannel, key.getMessage());
            if (!isResponseSuccess(response, key.getTransactionRole())) {
                // 触发注册失败
                rpcRemotingClient.onRegisterMsgFail(key.getAddress(), tmpChannel, response, key.getMessage());
            } else {
                channelToServer = tmpChannel;
                rpcRemotingClient.onRegisterMsgSuccess(key.getAddress(), tmpChannel, response,
                    key.getMessage());
            }
        } catch (Exception exx) {
            if (tmpChannel != null) { tmpChannel.close(); }
            throw new FrameworkException(
                "register error,role:" + key.getTransactionRole().name() + ",err:" + exx.getMessage());
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(
                "register success, cost " + (System.currentTimeMillis() - start) + " ms, version:"
                    + getVersion(response, key.getTransactionRole()) + ",role:" + key.getTransactionRole().name()
                    + ",channel:" + channelToServer);
        }
        return channelToServer;
    }

    /**
     * 判断消息发送是否成功
     * @param response
     * @param transactionRole
     * @return
     */
    private boolean isResponseSuccess(Object response, NettyPoolKey.TransactionRole transactionRole) {
        // 代表超时
        if (null == response) { return false; }
        // 如果对应的 连接为 TM 则必须返回TM 响应消息
        if (transactionRole.equals(NettyPoolKey.TransactionRole.TMROLE)) {
            if (!(response instanceof RegisterTMResponse)) {
                return false;
            }
            // 该方法总是返回true 这啥意思
            return ((RegisterTMResponse) response).isIdentified();
        } else if (transactionRole.equals(NettyPoolKey.TransactionRole.RMROLE)) {
            if (!(response instanceof RegisterRMResponse)) {
                return false;
            }
            return ((RegisterRMResponse) response).isIdentified();
        }
        return false;
    }

    /**
     * 从res 上获取 version信息
     * @param response
     * @param transactionRole
     * @return
     */
    private String getVersion(Object response, NettyPoolKey.TransactionRole transactionRole) {
        if (transactionRole.equals(NettyPoolKey.TransactionRole.TMROLE)) {
            return ((RegisterTMResponse)response).getVersion();
        } else {
            return ((RegisterRMResponse)response).getVersion();
        }
    }

    /**
     * 断开连接
     * @param key
     * @param channel
     * @throws Exception
     */
    @Override
    public void destroyObject(NettyPoolKey key, Channel channel) throws Exception {

        if (null != channel) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("will destroy channel:" + channel);
            }
            channel.disconnect();
            channel.close();
        }
    }

    /**
     * 确保 channe 存活
     * @param key
     * @param obj
     * @return
     */
    @Override
    public boolean validateObject(NettyPoolKey key, Channel obj) {
        if (null != obj && obj.isActive()) {
            return true;
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("channel valid false,channel:" + obj);
        }
        return false;
    }

    @Override
    public void activateObject(NettyPoolKey key, Channel obj) throws Exception {

    }

    @Override
    public void passivateObject(NettyPoolKey key, Channel obj) throws Exception {

    }
}
