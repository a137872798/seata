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
package io.seata.core.lock;

import java.util.List;

/**
 * The interface Locker.
 * 锁对象
 * @author zhangsen
 * @date 2019 -05-15
 */
public interface Locker {

    /**
     * Acquire lock boolean.
     * 尝试针对多 row 进行上锁 每行应该就是对应到 一次分布式事务中的 participant
     * @param rowLock the row lock
     * @return the boolean
     */
    boolean acquireLock(List<RowLock> rowLock) ;

    /**
     * Un lock boolean.
     * 代表事务完成 释放锁
     * @param rowLock the row lock
     * @return the boolean
     */
    boolean releaseLock(List<RowLock> rowLock);

    /**
     * Is lockable boolean.
     * 判断能否加锁
     * @param rowLock the row lock
     * @return the boolean
     */
    boolean isLockable(List<RowLock> rowLock);

    /**
     * Clean all locks boolean.
     * 关闭锁时什么意思???
     * @return the boolean
     */
    void cleanAllLocks();
}

