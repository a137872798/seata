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

import io.seata.common.exception.FrameworkErrorCode;
import io.seata.core.protocol.ResultCode;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.protocol.transaction.BranchCommitRequest;
import io.seata.core.protocol.transaction.BranchCommitResponse;
import io.seata.core.protocol.transaction.BranchRollbackRequest;
import io.seata.core.protocol.transaction.BranchRollbackResponse;
import io.seata.core.protocol.transaction.UndoLogDeleteRequest;
import io.seata.core.rpc.ClientMessageListener;
import io.seata.core.rpc.ClientMessageSender;
import io.seata.core.rpc.TransactionMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Rm message listener.
 * 针对rm 消息的监听器
 * @author jimin.jm @alibaba-inc.com
 * @date 2018 /10/11
 */
public class RmMessageListener implements ClientMessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RmMessageListener.class);

    /**
     * 处理事务消息
     */
    private TransactionMessageHandler handler;

    /**
     * Instantiates a new Rm message listener.
     *
     * @param handler the handler
     */
    public RmMessageListener(TransactionMessageHandler handler) {
        this.handler = handler;
    }

    /**
     * Sets handler.
     *
     * @param handler the handler
     */
    public void setHandler(TransactionMessageHandler handler) {
        this.handler = handler;
    }

    /**
     * 上层的 dispatch 会转发到下游 针对 client 就是使用MessageListener 去消费消息
     * @param request       the msg id
     * @param serverAddress the server address
     * @param sender        the sender
     */
    @Override
    public void onMessage(RpcMessage request, String serverAddress, ClientMessageSender sender) {
        Object msg = request.getBody();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("onMessage:" + msg);
        }
        // 如果是分支进行提交
        if (msg instanceof BranchCommitRequest) {
            // 处理 commit
            handleBranchCommit(request, serverAddress, (BranchCommitRequest)msg, sender);
            // 处理回滚
        } else if (msg instanceof BranchRollbackRequest) {
            handleBranchRollback(request, serverAddress, (BranchRollbackRequest)msg, sender);
            // 代表某个分布式事务完成后 清理日志信息
        }else if (msg instanceof UndoLogDeleteRequest) {
            handleUndoLogDelete((UndoLogDeleteRequest) msg);
        }
    }

    /**
     * 处理回滚
     * @param request
     * @param serverAddress
     * @param branchRollbackRequest
     * @param sender
     */
    private void handleBranchRollback(RpcMessage request, String serverAddress,
                                      BranchRollbackRequest branchRollbackRequest,
                                      ClientMessageSender sender) {
        BranchRollbackResponse resultMessage = null;
        resultMessage = (BranchRollbackResponse)handler.onRequest(branchRollbackRequest, null);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("branch rollback result:" + resultMessage);
        }
        try {
            // 返回结果
            sender.sendResponse(request, serverAddress, resultMessage);
        } catch (Throwable throwable) {
            LOGGER.error("send response error: {}", throwable.getMessage(), throwable);
        }
    }

    private void handleBranchCommit(RpcMessage request, String serverAddress,
                                    BranchCommitRequest branchCommitRequest,
                                    ClientMessageSender sender) {

        BranchCommitResponse resultMessage = null;
        try {
            resultMessage = (BranchCommitResponse)handler.onRequest(branchCommitRequest, null);
            sender.sendResponse(request, serverAddress, resultMessage);
        } catch (Exception e) {
            LOGGER.error(FrameworkErrorCode.NetOnMessage.getErrCode(), e.getMessage(), e);
            if (resultMessage == null) {
                resultMessage = new BranchCommitResponse();
            }
            // 提交失败时也要返回消息
            resultMessage.setResultCode(ResultCode.Failed);
            resultMessage.setMsg(e.getMessage());
            sender.sendResponse(request, serverAddress, resultMessage);
        }
    }

    private void handleUndoLogDelete(UndoLogDeleteRequest undoLogDeleteRequest) {
        try {
            handler.onRequest(undoLogDeleteRequest, null);
        } catch (Exception e) {
            LOGGER.error("Failed to delete undo log by undoLogDeleteRequest on" + undoLogDeleteRequest.getResourceId());
        }
    }
}
