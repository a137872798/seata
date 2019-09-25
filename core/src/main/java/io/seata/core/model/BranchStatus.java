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
package io.seata.core.model;

import java.util.HashMap;
import java.util.Map;

import io.seata.common.exception.ShouldNeverHappenException;

/**
 * Status of branch transaction.
 * 应该是participants 的状态
 * @author sharajava
 */
public enum BranchStatus {

    /**
     * The Unknown.
     * description:Unknown branch status.
     * 刚创建时应该是这个
     */
    Unknown(0),

    /**
     * The Registered.
     * description:Registered to TC.
     * 将participants 注册到协调者中
     */
    Registered(1),

    /**
     * The Phase one done.
     * description:Branch logic is successfully done at phase one.
     * 开始执行第一阶段 也就是 Try
     */
    PhaseOne_Done(2),

    /**
     * The Phase one failed.
     * description:Branch logic is failed at phase one.
     * 第一阶段执行失败
     */
    PhaseOne_Failed(3),

    /**
     * The Phase one timeout.
     * description:Branch logic is NOT reported for a timeout.
     * 第一阶段超时
     */
    PhaseOne_Timeout(4),

    /**
     * The Phase two committed.
     * description:Commit logic is successfully done at phase two.
     * 第二阶段成功提交
     */
    PhaseTwo_Committed(5),

    /**
     * The Phase two commit failed retryable.
     * description:Commit logic is failed but retryable.
     * 第二阶段提交失败进行重试  （一旦try 成功后就可以尝试进行 commit/rollback 当单次执行失败后可以 选择补偿或者不补偿）
     */
    PhaseTwo_CommitFailed_Retryable(6),

    /**
     * The Phase two commit failed unretryable.
     * description:Commit logic is failed and NOT retryable.
     * 第二阶段提交失败不允许重试
     */
    PhaseTwo_CommitFailed_Unretryable(7),


    // 回滚的相关方法

    /**
     * The Phase two rollbacked.
     * description:Rollback logic is successfully done at phase two.
     */
    PhaseTwo_Rollbacked(8),

    /**
     * The Phase two rollback failed retryable.
     * description:Rollback logic is failed but retryable.
     */
    PhaseTwo_RollbackFailed_Retryable(9),

    /**
     * The Phase two rollback failed unretryable.
     * description:Rollback logic is failed but NOT retryable.
     */
    PhaseTwo_RollbackFailed_Unretryable(10);

    private int code;

    BranchStatus(int code) {
        this.code = code;
    }

    /**
     * Gets code.
     *
     * @return the code
     */
    public int getCode() {
        return code;
    }

    /**
     * 使用全局单例保存 也算是一边of() 的简便实现吧
     */
    private static final Map<Integer, BranchStatus> MAP = new HashMap<>(values().length);

    static {
        for (BranchStatus status : values()) {
            MAP.put(status.getCode(), status);
        }
    }

    /**
     * Get branch status.
     *
     * @param code the code
     * @return the branch status
     */
    public static BranchStatus get(byte code) {
        return get((int)code);
    }

    /**
     * Get branch status.
     *
     * @param code the code
     * @return the branch status
     */
    public static BranchStatus get(int code) {
        BranchStatus status = MAP.get(code);

        if (null == status) {
            throw new ShouldNeverHappenException("Unknown BranchStatus[" + code + "]");
        }

        return status;
    }
}
