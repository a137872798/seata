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
package io.seata.server;

import io.seata.core.exception.AbstractExceptionHandler;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.GlobalStatus;
import io.seata.core.protocol.transaction.BranchRegisterRequest;
import io.seata.core.protocol.transaction.BranchRegisterResponse;
import io.seata.core.protocol.transaction.BranchReportRequest;
import io.seata.core.protocol.transaction.BranchReportResponse;
import io.seata.core.protocol.transaction.GlobalBeginRequest;
import io.seata.core.protocol.transaction.GlobalBeginResponse;
import io.seata.core.protocol.transaction.GlobalCommitRequest;
import io.seata.core.protocol.transaction.GlobalCommitResponse;
import io.seata.core.protocol.transaction.GlobalLockQueryRequest;
import io.seata.core.protocol.transaction.GlobalLockQueryResponse;
import io.seata.core.protocol.transaction.GlobalRollbackRequest;
import io.seata.core.protocol.transaction.GlobalRollbackResponse;
import io.seata.core.protocol.transaction.GlobalStatusRequest;
import io.seata.core.protocol.transaction.GlobalStatusResponse;
import io.seata.core.protocol.transaction.TCInboundHandler;
import io.seata.core.rpc.RpcContext;
import io.seata.server.session.GlobalSession;
import io.seata.server.session.SessionHolder;

/**
 * The type Abstract tc inbound handler.
 * TC 的输入处理器骨架类
 * @author sharajava
 */
public abstract class AbstractTCInboundHandler extends AbstractExceptionHandler implements TCInboundHandler {

    /**
     * 处理开启全局事务的请求
     * @param request
     * @param rpcContext  the rpc context
     * @return
     */
    @Override
    public GlobalBeginResponse handle(GlobalBeginRequest request, final RpcContext rpcContext) {
        // 生成一个响应结果
        GlobalBeginResponse response = new GlobalBeginResponse();
        // 执行处理模板开启全局事务
        exceptionHandleTemplate(new AbstractCallback<GlobalBeginRequest, GlobalBeginResponse>() {
            @Override
            public void execute(GlobalBeginRequest request, GlobalBeginResponse response) throws TransactionException {
                // 对应回调对象的执行逻辑
                doGlobalBegin(request, response, rpcContext);
            }
        }, request, response);
        return response;
    }

    /**
     * Do global begin.
     * 由子类实现
     * @param request    the request
     * @param response   the response
     * @param rpcContext the rpc context
     * @throws TransactionException the transaction exception
     */
    protected abstract void doGlobalBegin(GlobalBeginRequest request, GlobalBeginResponse response,
        RpcContext rpcContext) throws TransactionException;

    /**
     * 处理全局提交逻辑
     * @param request
     * @param rpcContext   the rpc context
     * @return
     */
    @Override
    public GlobalCommitResponse handle(GlobalCommitRequest request, final RpcContext rpcContext) {
        GlobalCommitResponse response = new GlobalCommitResponse();
        exceptionHandleTemplate(new AbstractCallback<GlobalCommitRequest, GlobalCommitResponse>() {
            @Override
            public void execute(GlobalCommitRequest request, GlobalCommitResponse response)
                throws TransactionException {
                doGlobalCommit(request, response, rpcContext);
            }
        }, request, response);
        return response;
    }

    /**
     * Do global commit.
     *
     * @param request    the request
     * @param response   the response
     * @param rpcContext the rpc context
     * @throws TransactionException the transaction exception
     */
    protected abstract void doGlobalCommit(GlobalCommitRequest request, GlobalCommitResponse response,
        RpcContext rpcContext) throws TransactionException;

    /**
     * 回滚全局事务
     * @param request
     * @param rpcContext     the rpc context
     * @return
     */
    @Override
    public GlobalRollbackResponse handle(GlobalRollbackRequest request, final RpcContext rpcContext) {
        GlobalRollbackResponse response = new GlobalRollbackResponse();
        exceptionHandleTemplate(new AbstractCallback<GlobalRollbackRequest, GlobalRollbackResponse>() {
            // 执行的逻辑就是 全局回滚
            @Override
            public void execute(GlobalRollbackRequest request, GlobalRollbackResponse response)
                throws TransactionException {
                doGlobalRollback(request, response, rpcContext);
            }

            /**
             * 当触发事务相关的异常时
             * @param request
             * @param response
             * @param tex
             */
            @Override
            public void onTransactionException(GlobalRollbackRequest request, GlobalRollbackResponse response,
                TransactionException tex) {
                // 这里是为响应对象设置code
                super.onTransactionException(request, response, tex);
                // 根据事务id 获取 全局session
                GlobalSession globalSession = SessionHolder.findGlobalSession(request.getXid());
                if (globalSession != null) {
                    response.setGlobalStatus(globalSession.getStatus());
                } else {
                    response.setGlobalStatus(GlobalStatus.Finished);
                }
            }

            @Override
            public void onException(GlobalRollbackRequest request, GlobalRollbackResponse response, Exception rex) {
                super.onException(request, response, rex);
                GlobalSession globalSession = SessionHolder.findGlobalSession(request.getXid());
                if (globalSession != null) {
                    response.setGlobalStatus(globalSession.getStatus());
                } else {
                    response.setGlobalStatus(GlobalStatus.Finished);
                }
            }
        }, request, response);
        return response;
    }

    /**
     * Do global rollback.
     *
     * @param request    the request
     * @param response   the response
     * @param rpcContext the rpc context
     * @throws TransactionException the transaction exception
     */
    protected abstract void doGlobalRollback(GlobalRollbackRequest request, GlobalRollbackResponse response,
        RpcContext rpcContext) throws TransactionException;

    @Override
    public BranchRegisterResponse handle(BranchRegisterRequest request, final RpcContext rpcContext) {
        BranchRegisterResponse response = new BranchRegisterResponse();
        exceptionHandleTemplate(new AbstractCallback<BranchRegisterRequest, BranchRegisterResponse>() {
            @Override
            public void execute(BranchRegisterRequest request, BranchRegisterResponse response)
                throws TransactionException {
                doBranchRegister(request, response, rpcContext);
            }
        }, request, response);
        return response;
    }

    /**
     * Do branch register.
     *
     * @param request    the request
     * @param response   the response
     * @param rpcContext the rpc context
     * @throws TransactionException the transaction exception
     */
    protected abstract void doBranchRegister(BranchRegisterRequest request, BranchRegisterResponse response,
        RpcContext rpcContext) throws TransactionException;

    @Override
    public BranchReportResponse handle(BranchReportRequest request, final RpcContext rpcContext) {
        BranchReportResponse response = new BranchReportResponse();
        exceptionHandleTemplate(new AbstractCallback<BranchReportRequest, BranchReportResponse>() {
            @Override
            public void execute(BranchReportRequest request, BranchReportResponse response)
                throws TransactionException {
                doBranchReport(request, response, rpcContext);
            }
        }, request, response);
        return response;
    }

    /**
     * Do branch report.
     *
     * @param request    the request
     * @param rpcContext the rpc context
     * @throws TransactionException the transaction exception
     */
    protected abstract void doBranchReport(BranchReportRequest request, BranchReportResponse response,
        RpcContext rpcContext)
        throws TransactionException;

    @Override
    public GlobalLockQueryResponse handle(GlobalLockQueryRequest request, final RpcContext rpcContext) {
        GlobalLockQueryResponse response = new GlobalLockQueryResponse();
        exceptionHandleTemplate(new AbstractCallback<GlobalLockQueryRequest, GlobalLockQueryResponse>() {
            @Override
            public void execute(GlobalLockQueryRequest request, GlobalLockQueryResponse response)
                throws TransactionException {
                doLockCheck(request, response, rpcContext);
            }
        }, request, response);
        return response;
    }

    /**
     * Do lock check.
     *
     * @param request    the request
     * @param response   the response
     * @param rpcContext the rpc context
     * @throws TransactionException the transaction exception
     */
    protected abstract void doLockCheck(GlobalLockQueryRequest request, GlobalLockQueryResponse response,
        RpcContext rpcContext) throws TransactionException;

    @Override
    public GlobalStatusResponse handle(GlobalStatusRequest request, final RpcContext rpcContext) {
        GlobalStatusResponse response = new GlobalStatusResponse();
        exceptionHandleTemplate(new AbstractCallback<GlobalStatusRequest, GlobalStatusResponse>() {
            @Override
            public void execute(GlobalStatusRequest request, GlobalStatusResponse response)
                throws TransactionException {
                doGlobalStatus(request, response, rpcContext);
            }
        }, request, response);
        return response;
    }

    /**
     * Do global status.
     *
     * @param request    the request
     * @param response   the response
     * @param rpcContext the rpc context
     * @throws TransactionException the transaction exception
     */
    protected abstract void doGlobalStatus(GlobalStatusRequest request, GlobalStatusResponse response,
        RpcContext rpcContext) throws TransactionException;

}
