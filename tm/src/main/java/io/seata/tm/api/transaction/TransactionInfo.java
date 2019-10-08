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

import java.io.Serializable;
import java.util.Set;

/**
 * 事务信息
 * @author guoyao
 * @date 2019/4/17
 */
public final class TransactionInfo implements Serializable {

    public static final int DEFAULT_TIME_OUT = 60000;

    private int timeOut;

    private String name;

    /**
     * 关联的回滚规则
     */
    private Set<RollbackRule> rollbackRules;

    public int getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<RollbackRule> getRollbackRules() {
        return rollbackRules;
    }

    public void setRollbackRules(Set<RollbackRule> rollbackRules) {
        this.rollbackRules = rollbackRules;
    }

    /**
     * 判断某个异常是否允许进行回滚
     * @param ex
     * @return
     */
    public boolean rollbackOn(Throwable ex) {

        RollbackRule winner = null;
        int deepest = Integer.MAX_VALUE;

        if (this.rollbackRules != null) {
            for (RollbackRule rule : this.rollbackRules) {
                int depth = rule.getDepth(ex);
                if (depth >= 0 && depth < deepest) {
                    deepest = depth;
                    winner = rule;
                }
            }
        }

        return winner == null || !(winner instanceof NoRollbackRule);
    }
}
