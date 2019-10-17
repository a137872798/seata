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
package io.seata.rm.tcc.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * the TCC parameters that need to be passed to  the BusinessActivityContext；
 * <p>
 * add this annotation on the parameters of the try method, and the parameters will be passed to  the
 * BusinessActivityContext
 * TCC 参数对象  需要通过 Context???
 * @author zhangsen
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER, ElementType.FIELD})
public @interface BusinessActionContextParameter {

    /**
     * parameter's name
     * 参数名
     * @return the string
     */
    String paramName() default "";

    /**
     * if it is a sharding param ?
     * 是否是共享参数
     * @return boolean boolean
     */
    boolean isShardingParam() default false;

    /**
     * Specify the index of the parameter in the List
     * 首先代表该注解修饰的是一个list 且index 对应的值是需要的参数
     * @return int int
     */
    int index() default -1;

    /**
     * if get the parameter from the property of the object ?
     * 是否该参数是从 某个object 的 属性中获取的
     * @return boolean boolean
     */
    boolean isParamInProperty() default false;
}