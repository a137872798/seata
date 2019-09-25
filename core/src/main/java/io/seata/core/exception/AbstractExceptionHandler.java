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
package io.seata.core.exception;

import io.seata.config.Configuration;
import io.seata.config.ConfigurationFactory;
import io.seata.core.protocol.ResultCode;
import io.seata.core.protocol.transaction.AbstractTransactionRequest;
import io.seata.core.protocol.transaction.AbstractTransactionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Abstract exception handler.
 * 异常处理器
 * @author sharajava
 */
public abstract class AbstractExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractExceptionHandler.class);

    /**
     * The constant CONFIG.
     * 全局唯一配置对象
     */
    protected static final Configuration CONFIG = ConfigurationFactory.getInstance();

    /**
     * The interface Callback.
     * 该回调参数 入参和 出参都是 TransactionRequest, Response
     * @param <T> the type parameter
     * @param <S> the type parameter
     */
    public interface Callback<T extends AbstractTransactionRequest, S extends AbstractTransactionResponse> {
        /**
         * Execute.
         * 执行时机触发的回调???
         * @param request  the request
         * @param response the response
         * @throws TransactionException the transaction exception
         */
        void execute(T request, S response) throws TransactionException;

        /**
         * on Success
         * 成功时触发
         * @param request
         * @param response
         */
        void onSuccess(T request, S response);

        /**
         * onTransactionException
         * 出现异常时触发的回调
         * @param request
         * @param response
         * @param exception
         */
        void onTransactionException(T request, S response, TransactionException exception);

        /**
         * on other exception
         * 当出现异常时触发
         * @param request
         * @param response
         * @param exception
         */
        void onException(T request, S response, Exception exception);

    }

    /**
     * 回调对象骨架类
     * @param <T>
     * @param <S>
     */
    public abstract class AbstractCallback<T extends AbstractTransactionRequest, S extends AbstractTransactionResponse>
        implements Callback<T, S> {

        /**
         * 默认情况下 成功将ResultCode 设置成 Success
         * @param request
         * @param response
         */
        @Override
        public void onSuccess(T request, S response) {
            response.setResultCode(ResultCode.Success);
        }

        /**
         * 事务异常 处理
         * @param request
         * @param response
         * @param tex
         */
        @Override
        public void onTransactionException(T request, S response,
            TransactionException tex) {
            // 设置事务 code 这样异常就可以直接定位到 事务对象
            response.setTransactionExceptionCode(tex.getCode());
            // 设置成 失败
            response.setResultCode(ResultCode.Failed);
            response.setMsg("TransactionException[" + tex.getMessage() + "]");
        }

        /**
         * 当遇到异常时触发
         * @param request
         * @param response
         * @param rex
         */
        @Override
        public void onException(T request, S response, Exception rex) {
            response.setResultCode(ResultCode.Failed);
            response.setMsg("RuntimeException[" + rex.getMessage() + "]");
        }
    }

    /**
     * Exception handle template.
     * 异常处理模板
     * @param callback the callback
     * @param request  the request
     * @param response the response
     */
    public void exceptionHandleTemplate(Callback callback, AbstractTransactionRequest request,
        AbstractTransactionResponse response) {
        try {
            // 为什么execute 方法 会放在 callback里
            callback.execute(request, response);
            callback.onSuccess(request, response);
        } catch (TransactionException tex) {
            LOGGER.error("Catch TransactionException while do RPC, request: {}", request, tex);
            callback.onTransactionException(request, response, tex);
        } catch (RuntimeException rex) {
            LOGGER.error("Catch RuntimeException while do RPC, request: {}", request, rex);
            callback.onException(request, response, rex);
        }
    }

}
