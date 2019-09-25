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
package io.seata.core.rpc;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.seata.common.Constants;

import io.netty.channel.Channel;
import io.seata.core.rpc.netty.NettyPoolKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type rpc context.
 * 一次远程通信的上下文对象
 * @author jimin.jm @alibaba-inc.com
 * @date 2018 /12/07
 */
public class RpcContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcContext.class);

    /**
     * 代表 该client 是什么角色 (请求发起方)
     * role 包含 TM,RM,SERVER
     */
    private NettyPoolKey.TransactionRole clientRole;

    /**
     * 本次 远程调用的版本号
     */
    private String version;

    /**
     * 标记应用的 唯一标识
     */
    private String applicationId;

    /**
     * 该应用所属 事务组
     */
    private String transactionServiceGroup;

    /**
     * 该client 的id
     */
    private String clientId;

    /**
     * 连接 2端的channel 对象
     */
    private Channel channel;

    /**
     * 资源信息  对应 RM
     */
    private Set<String> resourceSets;

    // 下面这几个容器 都会关联到某些全局容器
    /**
     * id
     */
    private ConcurrentMap<Channel, RpcContext> clientIDHolderMap;

    /**
     * tm
     */
    private ConcurrentMap<Integer, RpcContext> clientTMHolderMap;

    /**
     * dbkeyRm
     */
    private ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>> clientRMHolderMap;

    /**
     * Release.
     * 释放上下文内包含的channel 对象
     */
    public void release() {
        // 获取 channel 的远端端口
        Integer clientPort = getClientPortFromChannel(channel);
        if (clientIDHolderMap != null) {
            clientIDHolderMap = null;
        }
        // 使用对应的 map
        if (clientRole == NettyPoolKey.TransactionRole.TMROLE && clientTMHolderMap != null) {
            clientTMHolderMap.remove(clientPort);
            clientTMHolderMap = null;
        }
        // 情况 RM map
        if (clientRole == NettyPoolKey.TransactionRole.RMROLE && clientRMHolderMap != null) {
            for (Map<Integer, RpcContext> portMap : clientRMHolderMap.values()) {
                portMap.remove(clientPort);
            }
            clientRMHolderMap = null;
        }
        // 情况 resourceSet
        if (null != resourceSets) {
            resourceSets.clear();
        }
    }

    /**
     * Hold in client channels.
     * 约束 客户端channel 干啥用的???
     *
     * @param clientTMHolderMap the client tm holder map
     */
    public void holdInClientChannels(ConcurrentMap<Integer, RpcContext> clientTMHolderMap) {
        if (this.clientTMHolderMap != null) {
            throw new IllegalStateException();
        }
        this.clientTMHolderMap = clientTMHolderMap;
        Integer clientPort = getClientPortFromChannel(channel);
        // 获取本channel 对象 并设置到 Tm 容器中
        this.clientTMHolderMap.put(clientPort, this);
    }

    /**
     * Hold in identified channels.
     * 将数据填充到 idMap中
     * @param clientIDHolderMap the client id holder map
     */
    public void holdInIdentifiedChannels(ConcurrentMap<Channel, RpcContext> clientIDHolderMap) {
        if (this.clientIDHolderMap != null) {
            throw new IllegalStateException();
        }
        this.clientIDHolderMap = clientIDHolderMap;
        this.clientIDHolderMap.put(channel, this);
    }

    /**
     * Hold in resource manager channels.
     * 将数据填充到 rm中
     * @param resourceId the resource id
     * @param portMap    the client rm holder map
     */
    public void holdInResourceManagerChannels(String resourceId, ConcurrentMap<Integer, RpcContext> portMap) {
        if (null == this.clientRMHolderMap) {
            this.clientRMHolderMap = new ConcurrentHashMap<String, ConcurrentMap<Integer, RpcContext>>();
        }
        Integer clientPort = getClientPortFromChannel(channel);
        portMap.put(clientPort, this);
        this.clientRMHolderMap.put(resourceId, portMap);
    }

    /**
     * Hold in resource manager channels.
     *
     * @param resourceId the resource id
     * @param clientPort the client port
     */
    public void holdInResourceManagerChannels(String resourceId, Integer clientPort) {
        if (null == this.clientRMHolderMap) {
            this.clientRMHolderMap = new ConcurrentHashMap<String, ConcurrentMap<Integer, RpcContext>>();
        }
        clientRMHolderMap.putIfAbsent(resourceId, new ConcurrentHashMap<Integer, RpcContext>());
        ConcurrentMap<Integer, RpcContext> portMap = clientRMHolderMap.get(resourceId);
        portMap.put(clientPort, this);
    }

    /**
     * Gets get client rm holder map.
     *
     * @return the get client rm holder map
     */
    public ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>> getClientRMHolderMap() {
        return clientRMHolderMap;
    }

    /**
     * Gets port map.
     *
     * @param resourceId the resource id
     * @return the port map
     */
    public Map<Integer, RpcContext> getPortMap(String resourceId) {
        return clientRMHolderMap.get(resourceId);
    }

    /**
     * Gets get client id.
     *
     * @return the get client id
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Gets get channel.
     *
     * @return the get channel
     */
    public Channel getChannel() {
        return channel;
    }

    /**
     * Sets set channel.
     *
     * @param channel the channel
     */
    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    /**
     * Gets get application id.
     *
     * @return the get application id
     */
    public String getApplicationId() {
        return applicationId;
    }

    /**
     * Sets set application id.
     *
     * @param applicationId the application id
     */
    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    /**
     * Gets get transaction service group.
     *
     * @return the get transaction service group
     */
    public String getTransactionServiceGroup() {
        return transactionServiceGroup;
    }

    /**
     * Sets set transaction service group.
     *
     * @param transactionServiceGroup the transaction service group
     */
    public void setTransactionServiceGroup(String transactionServiceGroup) {
        this.transactionServiceGroup = transactionServiceGroup;
    }

    /**
     * Gets get client role.
     *
     * @return the get client role
     */
    public NettyPoolKey.TransactionRole getClientRole() {
        return clientRole;
    }

    /**
     * Sets set client role.
     *
     * @param clientRole the client role
     */
    public void setClientRole(NettyPoolKey.TransactionRole clientRole) {
        this.clientRole = clientRole;
    }

    /**
     * Gets get version.
     *
     * @return the get version
     */
    public String getVersion() {
        return version;
    }

    /**
     * Sets set version.
     *
     * @param version the version
     */
    public void setVersion(String version) {
        this.version = version;
    }

    /**
     * 从channel 上获取 远端地址信息
     * @param channel
     * @return
     */
    private static String getAddressFromChannel(Channel channel) {
        SocketAddress socketAddress = channel.remoteAddress();
        String address = socketAddress.toString();
        if (socketAddress.toString().indexOf(Constants.ENDPOINT_BEGIN_CHAR) == 0) {
            address = socketAddress.toString().substring(Constants.ENDPOINT_BEGIN_CHAR.length());
        }
        return address;
    }

    /**
     * 从channel 上获取端口信息
     * @param channel
     * @return
     */
    private static Integer getClientPortFromChannel(Channel channel) {
        String address = getAddressFromChannel(channel);
        Integer port = 0;
        try {
            // 如果存在 ":" 获取后面那部分作为 port
            if (address.contains(Constants.IP_PORT_SPLIT_CHAR)) {
                port = Integer.parseInt(address.substring(address.lastIndexOf(Constants.IP_PORT_SPLIT_CHAR) + 1));
            }
        } catch (NumberFormatException exx) {
            LOGGER.error(exx.getMessage());
        }
        return port;
    }

    /**
     * Gets get resource sets.
     *
     * @return the get resource sets
     */
    public Set<String> getResourceSets() {
        return resourceSets;
    }

    /**
     * Sets set resource sets.
     *
     * @param resourceSets the resource sets
     */
    public void setResourceSets(Set<String> resourceSets) {
        this.resourceSets = resourceSets;
    }

    /**
     * Add resource.
     *
     * @param resource the resource
     */
    public void addResource(String resource) {
        if (null == resource) {
            this.resourceSets = new HashSet<String>();
        }
        this.resourceSets.add(resource);
    }

    /**
     * Add resources.
     *
     * @param resource the resource
     */
    public void addResources(Set<String> resource) {
        if (null == resource) { return; }
        if (null == resourceSets) {
            this.resourceSets = new HashSet<String>();
        }
        this.resourceSets.addAll(resource);
    }

    /**
     * Sets client id.
     *
     * @param clientId the client id
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public String toString() {
        return "RpcContext{" +
            "applicationId='" + applicationId + '\'' +
            ", transactionServiceGroup='" + transactionServiceGroup + '\'' +
            ", clientId='" + clientId + '\'' +
            ", channel=" + channel +
            ", resourceSets=" + resourceSets +
            '}';
    }
}
