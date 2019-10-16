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
package io.seata.rm;

import java.util.concurrent.Callable;

import io.seata.core.context.RootContext;

/**
 * Template of executing business logic in a local transaction with Global lock.
 * 基于 GlobalLock 注解 的本地事务 （使用该事务的注解会确保 能够读取到全局事务修改完成后的最新数据）
 * @param <T>
 * @author deyou
 * @date 2019.03.07
 */
public class GlobalLockTemplate<T> {

    /**
     * Execute object.
     * 执行回调函数
     * @param business the business
     * @return the object
     * @throws Exception
     */
    public Object execute(Callable<T> business) throws Exception {

        Object rs = null;
        try {
            // add global lock declare
            // 在本地线程上下文中绑定 需要获取全局锁
            RootContext.bindGlobalLockFlag();

            // Do Your Business
            rs = business.call();
        } finally {
            //clean the global lock declare
            // 解除绑定
            RootContext.unbindGlobalLockFlag();
        }

        return rs;
    }

}
