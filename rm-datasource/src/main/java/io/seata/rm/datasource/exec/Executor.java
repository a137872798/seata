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
package io.seata.rm.datasource.exec;

/**
 * The interface Executor.
 *
 * @author sharajava
 * 执行者接口 不同于 JDK的 Executor   具备接受参数 并生成对应结果的 方法
 * @param <T> the type parameter
 */
public interface Executor<T> {

    /**
     * Execute t.
     *
     * @param args the args
     * @return the t
     * @throws Throwable the throwable
     */
    T execute(Object... args) throws Throwable;
}
