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
package io.seata.rm.tcc.remoting;

import io.seata.common.exception.FrameworkException;

/**
 * extract remoting bean info
 * 远程bean 解析器
 * @author zhangsen
 */
public interface RemotingParser {

    /**
     * if it is remoting bean ?
     * 判断某个bean 是否是远程 rpc框架中实现的bean
     * @param bean     the bean
     * @param beanName the bean name
     * @return boolean boolean
     * @throws FrameworkException the framework exception
     */
    boolean isRemoting(Object bean, String beanName) throws FrameworkException;

    /**
     * if it is reference bean ?
     * 是否是 服务提供者
     * @param bean     the bean
     * @param beanName the bean name
     * @return boolean boolean
     * @throws FrameworkException the framework exception
     */
    boolean isReference(Object bean, String beanName) throws FrameworkException;

    /**
     * if it is service bean ?
     * 是否是服务消费者
     * @param bean     the bean
     * @param beanName the bean name
     * @return boolean boolean
     * @throws FrameworkException the framework exception
     */
    boolean isService(Object bean, String beanName) throws FrameworkException;

    /**
     * get the remoting bean info
     * 获取信息
     * @param bean     the bean
     * @param beanName the bean name
     * @return service desc
     * @throws FrameworkException the framework exception
     */
    RemotingDesc getServiceDesc(Object bean, String beanName) throws FrameworkException;

    /**
     * the remoting protocol
     *
     * @return protocol
     */
    short getProtocol();


}
