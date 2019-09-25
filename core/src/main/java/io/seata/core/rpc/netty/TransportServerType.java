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

/**
 * The enum Transport server type.
 * 服务端传输类型
 * @author jimin.jm @alibaba-inc.com
 * @date 2018 /9/10
 */
public enum TransportServerType {
    /**
     * Native transport server type.
     * 啥意思啊 代表client 和server 在同一台机器上吗???
     */
    NATIVE("native"),
    /**
     * Nio transport server type.
     * 使用 nio 进行通信
     */
    NIO("nio");

    /**
     * The Name.
     */
    public final String name;

    TransportServerType(String name) {
        this.name = name;
    }
}
