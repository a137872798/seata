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

/**
 * The interface Channel event listener.
 * channel 事件监听对象
 * @author jimin.jm @alibaba-inc.com
 * @date 2018 /9/12
 */
public interface ChannelEventListener {
    /**
     * On channel connect.
     * 当channel 连接到某个地址时触发
     * @param remoteAddr the remote addr
     * @param channel    the channel
     */
    void onChannelConnect(final String remoteAddr, final Channel channel);

    /**
     * On channel close.
     * 当channel 关闭时触发
     * @param remoteAddr the remote addr
     * @param channel    the channel
     */
    void onChannelClose(final String remoteAddr, final Channel channel);

    /**
     * On channel exception.
     * channel 异常时触发
     * @param remoteAddr the remote addr
     * @param channel    the channel
     */
    void onChannelException(final String remoteAddr, final Channel channel);

    /**
     * On channel idle.
     * channel  空闲时触发
     * @param remoteAddr the remote addr
     * @param channel    the channel
     */
    void onChannelIdle(final String remoteAddr, final Channel channel);
}
