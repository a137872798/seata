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
package io.seata.rm.tcc.interceptor;

import com.alibaba.fastjson.JSON;
import io.seata.common.Constants;
import io.seata.common.exception.FrameworkException;
import io.seata.common.executor.Callback;
import io.seata.common.util.NetUtil;
import io.seata.core.model.BranchType;
import io.seata.rm.DefaultResourceManager;
import io.seata.rm.tcc.api.BusinessActionContext;
import io.seata.rm.tcc.api.BusinessActionContextParameter;
import io.seata.rm.tcc.api.TwoPhaseBusinessAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handler the TCC Participant Aspect : Setting Context, Creating Branch Record
 * TCC 拦截处理器对象
 * @author zhangsen
 */
public class ActionInterceptorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActionInterceptorHandler.class);

    /**
     * Handler the TCC Aspect
     *
     * @param method         the method  被调用的方法
     * @param arguments      the arguments  入参
     * @param businessAction the business action  全局事务id
     * @param targetCallback the target callback   真正执行事务的方法
     * @return map map
     * @throws Throwable the throwable
     */
    public Map<String, Object> proceed(Method method, Object[] arguments, String xid, TwoPhaseBusinessAction businessAction,
                                       Callback<Object> targetCallback) throws Throwable {
        Map<String, Object> ret = new HashMap<String, Object>(16);

        //TCC name
        String actionName = businessAction.name();
        BusinessActionContext actionContext = new BusinessActionContext();
        actionContext.setXid(xid);
        //set action name
        actionContext.setActionName(actionName);
        //TODO services

        //Creating Branch Record
        // 创建一个分事务 并通过RM 保存到TC 上
        String branchId = doTccActionLogStore(method, arguments, businessAction, actionContext);
        actionContext.setBranchId(branchId);

        //set the parameter whose type is BusinessActionContext
        // 从方法中找到 BusinessActionContext 类型的参数
        Class<?>[] types = method.getParameterTypes();
        int argIndex = 0;
        for (Class<?> cls : types) {
            if (cls.getName().equals(BusinessActionContext.class.getName())) {
                arguments[argIndex] = actionContext;
                break;
            }
            argIndex++;
        }
        //the final parameters of the try method
        // 保存调用参数
        ret.put(Constants.TCC_METHOD_ARGUMENTS, arguments);
        //the final result
        // 保存执行结果
        ret.put(Constants.TCC_METHOD_RESULT, targetCallback.execute());
        return ret;
    }

    /**
     * Creating Branch Record
     * 创建分事务
     * @param method         the method
     * @param arguments      the arguments
     * @param businessAction the business action
     * @param actionContext  the action context 等待填装数据的上下文
     * @return the string
     */
    protected String doTccActionLogStore(Method method, Object[] arguments, TwoPhaseBusinessAction businessAction,
                                         BusinessActionContext actionContext) {
        String actionName = actionContext.getActionName();
        String xid = actionContext.getXid();
        // 从方法签名中找到需要的参数（BusinessActionContextParameter 中包含的参数） 并添加到 context 中
        Map<String, Object> context = fetchActionRequestContext(method, arguments);
        context.put(Constants.ACTION_START_TIME, System.currentTimeMillis());

        //init business context 就是找到 T C C 3个方法并设置到 context 中
        initBusinessContext(context, method, businessAction);
        //Init running environment context  将本地ip 设置到 context 中
        initFrameworkContext(context);
        // 将参数设置到
        actionContext.setActionContext(context);

        //init applicationData
        Map<String, Object> applicationContext = new HashMap<String, Object>(4);
        // 代表处理 TCC 的上下文
        applicationContext.put(Constants.TCC_ACTION_CONTEXT, context);
        String applicationContextStr = JSON.toJSONString(applicationContext);
        try {
            //registry branch record
            Long branchId = DefaultResourceManager.get().branchRegister(BranchType.TCC, actionName, null, xid,
                applicationContextStr, null);
            return String.valueOf(branchId);
        } catch (Throwable t) {
            String msg = "TCC branch Register error, xid:" + xid;
            LOGGER.error(msg, t);
            throw new FrameworkException(t, msg);
        }
    }

    /**
     * Init running environment context
     *
     * @param context the context
     */
    protected void initFrameworkContext(Map<String, Object> context) {
        try {
            context.put(Constants.HOST_NAME, NetUtil.getLocalIp());
        } catch (Throwable t) {
            LOGGER.warn("getLocalIP error", t);
        }
    }

    /**
     * Init business context
     * 初始化 业务上下文  携带 TwoPhaseBusinessAction 注解的方法本身被看作是 prepare 方法
     * @param context        the context
     * @param method         the method
     * @param businessAction the business action
     */
    protected void initBusinessContext(Map<String, Object> context, Method method,
                                       TwoPhaseBusinessAction businessAction) {
        if (method != null) {
            //the phase one method name
            context.put(Constants.PREPARE_METHOD, method.getName());
        }
        if (businessAction != null) {
            //the phase two method name
            // 设置 confirm 和 rollback 的方法名
            context.put(Constants.COMMIT_METHOD, businessAction.commitMethod());
            context.put(Constants.ROLLBACK_METHOD, businessAction.rollbackMethod());
            context.put(Constants.ACTION_NAME, businessAction.name());
        }
    }

    /**
     * Extracting context data from parameters, add them to the context
     * 从参数中提取出上下文需要的东西
     * @param method    the method
     * @param arguments the arguments
     * @return map map
     */
    protected Map<String, Object> fetchActionRequestContext(Method method, Object[] arguments) {
        Map<String, Object> context = new HashMap<String, Object>(8);

        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        // 寻找携带 BusinessActionContextParameter 的参数
        for (int i = 0; i < parameterAnnotations.length; i++) {
            for (int j = 0; j < parameterAnnotations[i].length; j++) {
                if (parameterAnnotations[i][j] instanceof BusinessActionContextParameter) {
                    BusinessActionContextParameter param = (BusinessActionContextParameter)parameterAnnotations[i][j];
                    if (null == arguments[i]) {
                        throw new IllegalArgumentException("@BusinessActionContextParameter 's params can not null");
                    }
                    Object paramObject = arguments[i];
                    // 如果注解设置了 index 属性代表是一个list
                    int index = param.index();
                    //List, get by index
                    if (index >= 0) {
                        // 获取List 对应下标的 参数
                        Object targetParam = ((List<Object>)paramObject).get(index);
                        // 代表是否要从该参数中抽取属性
                        if (param.isParamInProperty()) {
                            context.putAll(ActionContextUtil.fetchContextFromObject(targetParam));
                        } else {
                            // 正常情况将参数设置到 context 中
                            context.put(param.paramName(), targetParam);
                        }
                    // 如果只是普通对象 同样根据情况判断是否要加入到 context 中
                    } else {
                        if (param.isParamInProperty()) {
                            context.putAll(ActionContextUtil.fetchContextFromObject(paramObject));
                        } else {
                            context.put(param.paramName(), paramObject);
                        }
                    }
                }
            }
        }
        return context;
    }

}
