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
 * The type Abstract transaction request to tc.
 * 发往TC 的请求对象  现在看来 TC 是管理 协调者的 RM 是管理参与者的(还有事务日志)
 * @author sharajava
 */
public abstract class AbstractTransactionRequestToTC extends AbstractTransactionRequest {

    /**
     * The Handler.
     * TC 请求处理器
     */
    protected TCInboundHandler handler;

    /**
     * Sets tc inbound handler.
     *
     * @param handler the handler
     */
    public void setTCInboundHandler(TCInboundHandler handler) {
        this.handler = handler;
    }
}