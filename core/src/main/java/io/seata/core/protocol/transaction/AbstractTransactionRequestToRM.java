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
package io.seata.core.protocol.transaction;


/**
 * The type Abstract transaction request to rm.
 * 事务对象 与 RM 间的请求体
 * @author sharajava
 */
public abstract class AbstractTransactionRequestToRM extends AbstractTransactionRequest {

    /**
     * The Handler.
     * 使用 RMInbound 处理传入消息  为什么会附属于请求对象上
     */
    protected RMInboundHandler handler;

    /**
     * Sets rm inbound message handler.
     *
     * @param handler the handler
     */
    public void setRMInboundMessageHandler(RMInboundHandler handler) {
        this.handler = handler;
    }
}
