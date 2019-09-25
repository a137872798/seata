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


import io.seata.core.protocol.AbstractMessage;
import io.seata.core.rpc.RpcContext;

/**
 * The type Abstract transaction request.
 * 事务请求对象
 * @author sharajava
 */
public abstract class AbstractTransactionRequest extends AbstractMessage {

    /**
     * Handle abstract transaction response.
     * 处理分布式中上下文信息并返回结果对象
     * @param rpcContext the rpc context
     * @return the abstract transaction response
     */
    public abstract AbstractTransactionResponse handle(RpcContext rpcContext);
}
