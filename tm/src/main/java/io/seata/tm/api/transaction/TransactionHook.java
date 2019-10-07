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
package io.seata.tm.api.transaction;

/**
 * 事务钩子对象
 * @author guoyao
 * @date 2019/3/4
 */
public interface TransactionHook {

    /**
     * before tx begin
     * 在事务开始前执行
     */
    void beforeBegin();

    /**
     * after tx begin
     * 开始后执行
     */
    void afterBegin();

    /**
     * before tx commit
     * 提交前执行
     */
    void beforeCommit();

    /**
     * after tx commit
     * 提交后执行
     */
    void afterCommit();

    /**
     * before tx rollback
     */
    void beforeRollback();

    /**
     * after tx rollback
     */
    void afterRollback();

    /**
     * after tx all Completed
     */
    void afterCompletion();
}
