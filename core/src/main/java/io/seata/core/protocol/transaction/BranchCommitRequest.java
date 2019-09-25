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

import io.seata.core.protocol.MessageType;
import io.seata.core.rpc.RpcContext;

/**
 * The type Branch commit request.
 * 代表提交分支的请求  注意该请求是发往RM 的
 *
 * @author sharajava
 */
public class BranchCommitRequest extends AbstractBranchEndRequest {

    @Override
    public short getTypeCode() {
        return MessageType.TYPE_BRANCH_COMMIT;
    }

    /**
     * 默认使用 本对象携带的 handler 处理自身
     * @param rpcContext the rpc context
     * @return
     */
    @Override
    public AbstractTransactionResponse handle(RpcContext rpcContext) {
        return handler.handle(this);
    }


}
