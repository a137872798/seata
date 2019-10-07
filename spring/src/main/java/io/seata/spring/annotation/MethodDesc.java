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
package io.seata.spring.annotation;

import java.lang.reflect.Method;

/**
 * The type Method desc.
 * 方法描述信息
 * @author jimin.jm @alibaba-inc.com
 * @date 2018 /12/28
 */
public class MethodDesc {
    /**
     * 该方法上标注的  全局事务注解
     */
    private GlobalTransactional transactionAnnotation;
    /**
     * 方法对象本身
     */
    private Method method;

    /**
     * Instantiates a new Method desc.
     *
     * @param transactionAnnotation the transaction annotation
     * @param method                the method
     */
    public MethodDesc(GlobalTransactional transactionAnnotation, Method method) {
        this.transactionAnnotation = transactionAnnotation;
        this.method = method;
    }

    /**
     * Gets transaction annotation.
     *
     * @return the transaction annotation
     */
    public GlobalTransactional getTransactionAnnotation() {
        return transactionAnnotation;
    }

    /**
     * Sets transaction annotation.
     *
     * @param transactionAnnotation the transaction annotation
     */
    public void setTransactionAnnotation(GlobalTransactional transactionAnnotation) {
        this.transactionAnnotation = transactionAnnotation;
    }

    /**
     * Gets method.
     *
     * @return the method
     */
    public Method getMethod() {
        return method;
    }

    /**
     * Sets method.
     *
     * @param method the method
     */
    public void setMethod(Method method) {
        this.method = method;
    }
}
