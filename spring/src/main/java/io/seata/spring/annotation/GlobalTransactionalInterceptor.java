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

import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.util.StringUtils;
import io.seata.rm.GlobalLockTemplate;
import io.seata.tm.api.DefaultFailureHandlerImpl;
import io.seata.tm.api.FailureHandler;
import io.seata.tm.api.TransactionalExecutor;
import io.seata.tm.api.TransactionalTemplate;
import io.seata.tm.api.transaction.NoRollbackRule;
import io.seata.tm.api.transaction.RollbackRule;
import io.seata.tm.api.transaction.TransactionInfo;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.BridgeMethodResolver;
import org.springframework.util.ClassUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The type Global transactional interceptor.
 * 拦截器对象 scanner 对象会将携带 GlobalTransactional 注解相关的类用该对象进行处理
 * @author jimin.jm @alibaba-inc.com
 */
public class GlobalTransactionalInterceptor implements MethodInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalTransactionalInterceptor.class);
    /**
     * 失败处理器
     */
    private static final FailureHandler DEFAULT_FAIL_HANDLER = new DefaultFailureHandlerImpl();

    /**
     * 事务模板对象 内部定义了 全局事务的一整套流程
     */
    private final TransactionalTemplate transactionalTemplate = new TransactionalTemplate();
    /**
     * 全局锁模板
     */
    private final GlobalLockTemplate<Object> globalLockTemplate = new GlobalLockTemplate<>();
    /**
     * 失败处理器
     */
    private final FailureHandler failureHandler;

    /**
     * Instantiates a new Global transactional interceptor.
     *
     * @param failureHandler the failure handler
     */
    public GlobalTransactionalInterceptor(FailureHandler failureHandler) {
        this.failureHandler = failureHandler == null ? DEFAULT_FAIL_HANDLER : failureHandler;

    }

    /**
     * 拦截aop 执行的方法
     * @param methodInvocation
     * @return
     * @throws Throwable
     */
    @Override
    public Object invoke(final MethodInvocation methodInvocation) throws Throwable {
        // 获取被拦截的目标类
        Class<?> targetClass = (methodInvocation.getThis() != null ? AopUtils.getTargetClass(methodInvocation.getThis()) : null);
        // 如果该方法存在重写版本 尽可能获取重写版本
        Method specificMethod = ClassUtils.getMostSpecificMethod(methodInvocation.getMethod(), targetClass);
        // 如果存在桥接方法的情况下 返回桥接方法
        final Method method = BridgeMethodResolver.findBridgedMethod(specificMethod);

        // 获取方法上的 注解
        final GlobalTransactional globalTransactionalAnnotation = getAnnotation(method, GlobalTransactional.class);
        // 该注解声明了 本事务 虽然不在全局事务中执行 但是必须确保关联的数据 如果正在某个全局事务中 必须对它可见（也就是等待全局事务更新完成）
        final GlobalLock globalLockAnnotation = getAnnotation(method, GlobalLock.class);
        if (globalTransactionalAnnotation != null) {
            // 处理全局事务
            return handleGlobalTransaction(methodInvocation, globalTransactionalAnnotation);
        } else if (globalLockAnnotation != null) {
            // 处理全局锁
            return handleGlobalLock(methodInvocation);
        } else {
            // 正常执行
            return methodInvocation.proceed();
        }
    }

    /**
     * 针对使用 @GlobalLock 的方法进行增强
     * @param methodInvocation
     * @return
     * @throws Exception
     */
    private Object handleGlobalLock(final MethodInvocation methodInvocation) throws Exception {
        // 使用模板执行
        return globalLockTemplate.execute(() -> {
            try {
                return methodInvocation.proceed();
            } catch (Throwable e) {
                if (e instanceof Exception) {
                    throw (Exception)e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    /**
     * 针对使用 @GlobalTransactional 注解的方法进行增强
     * @param methodInvocation
     * @param globalTrxAnno
     * @return
     * @throws Throwable
     */
    private Object handleGlobalTransaction(final MethodInvocation methodInvocation,
                                           final GlobalTransactional globalTrxAnno) throws Throwable {
        try {
            // 返回一个使用模板修饰的 对象  这里传入一个回调对象封装了方法真正执行的逻辑
            return transactionalTemplate.execute(new TransactionalExecutor() {
                @Override
                public Object execute() throws Throwable {
                    return methodInvocation.proceed();
                }

                // 获取事务注解上的名字
                public String name() {
                    String name = globalTrxAnno.name();
                    if (!StringUtils.isNullOrEmpty(name)) {
                        return name;
                    }
                    // 将方法名转换成名字
                    return formatMethod(methodInvocation.getMethod());
                }

                /**
                 * 获取事务信息
                 * @return
                 */
                @Override
                public TransactionInfo getTransactionInfo() {
                    TransactionInfo transactionInfo = new TransactionInfo();
                    transactionInfo.setTimeOut(globalTrxAnno.timeoutMills());
                    transactionInfo.setName(name());
                    Set<RollbackRule> rollbackRules = new LinkedHashSet<>();
                    for (Class<?> rbRule : globalTrxAnno.rollbackFor()) {
                        rollbackRules.add(new RollbackRule(rbRule));
                    }
                    for (String rbRule : globalTrxAnno.rollbackForClassName()) {
                        rollbackRules.add(new RollbackRule(rbRule));
                    }
                    for (Class<?> rbRule : globalTrxAnno.noRollbackFor()) {
                        rollbackRules.add(new NoRollbackRule(rbRule));
                    }
                    for (String rbRule : globalTrxAnno.noRollbackForClassName()) {
                        rollbackRules.add(new NoRollbackRule(rbRule));
                    }
                    transactionInfo.setRollbackRules(rollbackRules);
                    return transactionInfo;
                }
            });
            // 根据返回的事务执行异常走不同的逻辑
        } catch (TransactionalExecutor.ExecutionException e) {
            TransactionalExecutor.Code code = e.getCode();
            switch (code) {
                case RollbackDone:
                    throw e.getOriginalException();
                case BeginFailure:
                    failureHandler.onBeginFailure(e.getTransaction(), e.getCause());
                    throw e.getCause();
                case CommitFailure:
                    failureHandler.onCommitFailure(e.getTransaction(), e.getCause());
                    throw e.getCause();
                case RollbackFailure:
                    failureHandler.onRollbackFailure(e.getTransaction(), e.getCause());
                    throw e.getCause();
                default:
                    throw new ShouldNeverHappenException("Unknown TransactionalExecutor.Code: " + code);

            }
        }
    }

    private <T extends Annotation> T getAnnotation(Method method, Class<T> clazz) {
        return method == null ? null : method.getAnnotation(clazz);
    }

    private String formatMethod(Method method) {
        String paramTypes = Arrays.stream(method.getParameterTypes())
                .map(Class::getName)
                .collect(Collectors.joining(", ", "(", ")"));
        return method.getName() + paramTypes;
    }
}
