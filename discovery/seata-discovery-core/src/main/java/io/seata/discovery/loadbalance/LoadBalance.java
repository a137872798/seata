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
package io.seata.discovery.loadbalance;

import java.util.List;

/**
 * The interface Load balance.
 * 均衡负载接口
 * @author jimin.jm @alibaba-inc.com
 * @date 2019 /02/12
 */
public interface LoadBalance {

    /**
     * Select t.
     * 在一组invokers 中选择某个对象
     * @param <T>      the type parameter
     * @param invokers the invokers
     * @return the t
     * @throws Exception the exception
     */
    <T> T select(List<T> invokers) throws Exception;
}
