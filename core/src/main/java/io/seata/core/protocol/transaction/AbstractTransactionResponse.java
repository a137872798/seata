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


import io.seata.core.exception.TransactionExceptionCode;
import io.seata.core.protocol.AbstractResultMessage;

/**
 * The type Abstract transaction response.
 * 对应结果实体 该对象跟 req 对象继承的类不同 req 继承的是AbstractMessage
 *
 * @author sharajava
 */
public abstract class AbstractTransactionResponse extends AbstractResultMessage {

    /**
     * 默认使用的事务异常code 为 Unknown
     */
    private TransactionExceptionCode transactionExceptionCode = TransactionExceptionCode.Unknown;

    /**
     * Gets transaction exception code.
     *
     * @return the transaction exception code
     */
    public TransactionExceptionCode getTransactionExceptionCode() {
        return transactionExceptionCode;
    }

    /**
     * Sets transaction exception code.
     *
     * @param transactionExceptionCode the transaction exception code
     */
    public void setTransactionExceptionCode(TransactionExceptionCode transactionExceptionCode) {
        this.transactionExceptionCode = transactionExceptionCode;
    }

}
