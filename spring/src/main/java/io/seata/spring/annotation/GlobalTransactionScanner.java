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

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import io.seata.common.util.StringUtils;
import io.seata.config.ConfigurationFactory;
import io.seata.core.rpc.netty.RmRpcClient;
import io.seata.core.rpc.netty.ShutdownHook;
import io.seata.core.rpc.netty.TmRpcClient;
import io.seata.rm.RMClient;
import io.seata.rm.datasource.DataSourceProxy;
import io.seata.spring.annotation.datasource.DataSourceProxyHolder;
import io.seata.spring.tcc.TccActionInterceptor;
import io.seata.spring.util.SpringProxyUtils;
import io.seata.spring.util.TCCBeanParserUtils;
import io.seata.tm.TMClient;
import io.seata.tm.api.DefaultFailureHandlerImpl;
import io.seata.tm.api.FailureHandler;
import org.aopalliance.intercept.MethodInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.Advisor;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.AdvisedSupport;
import org.springframework.aop.framework.autoproxy.AbstractAutoProxyCreator;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;

import static io.seata.core.constants.ConfigurationKeys.DATASOURCE_AUTOPROXY;

/**
 * The type Global transaction scanner.
 * 全局事务扫描对象
 * @author jimin.jm @alibaba-inc.com
 * @date 2018 /12/28
 */
public class GlobalTransactionScanner extends AbstractAutoProxyCreator
    implements InitializingBean, ApplicationContextAware,
    DisposableBean, BeanPostProcessor {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalTransactionScanner.class);

    private static final int AT_MODE = 1;
    private static final int MT_MODE = 2;

    private static final int ORDER_NUM = 1024;
    private static final int DEFAULT_MODE = AT_MODE + MT_MODE;

    /**
     * 一组代理对象
     */
    private static final Set<String> PROXYED_SET = new HashSet<>();
    /**
     * 失败处理器
     */
    private static final FailureHandler DEFAULT_FAIL_HANDLER = new DefaultFailureHandlerImpl();

    /**
     * 方法拦截器
     */
    private MethodInterceptor interceptor;

    private final String applicationId;
    private final String txServiceGroup;
    private final int mode;
    private final boolean disableGlobalTransaction =
        ConfigurationFactory.getInstance().getBoolean("service.disableGlobalTransaction", false);

    private final FailureHandler failureHandlerHook;

    private ApplicationContext applicationContext;

    /**
     * Instantiates a new Global transaction scanner.
     * 通过指定的事务组 初始化
     * @param txServiceGroup the tx service group
     */
    public GlobalTransactionScanner(String txServiceGroup) {
        this(txServiceGroup, txServiceGroup, DEFAULT_MODE);
    }

    /**
     * Instantiates a new Global transaction scanner.
     *
     * @param txServiceGroup the tx service group
     * @param mode           the mode
     */
    public GlobalTransactionScanner(String txServiceGroup, int mode) {
        this(txServiceGroup, txServiceGroup, mode);
    }

    /**
     * Instantiates a new Global transaction scanner.
     *
     * @param applicationId  the application id
     * @param txServiceGroup the default server group
     */
    public GlobalTransactionScanner(String applicationId, String txServiceGroup) {
        this(applicationId, txServiceGroup, DEFAULT_MODE);
    }

    /**
     * Instantiates a new Global transaction scanner.
     *
     * @param applicationId  the application id  默认情况下 应用id 就是事务组名
     * @param txServiceGroup the tx service group  使用的模式
     * @param mode           the mode
     */
    public GlobalTransactionScanner(String applicationId, String txServiceGroup, int mode) {
        this(applicationId, txServiceGroup, mode, DEFAULT_FAIL_HANDLER);
    }

    /**
     * Instantiates a new Global transaction scanner.
     *
     * @param applicationId      the application id
     * @param txServiceGroup     the tx service group
     * @param failureHandlerHook the failure handler hook
     */
    public GlobalTransactionScanner(String applicationId, String txServiceGroup, FailureHandler failureHandlerHook) {
        this(applicationId, txServiceGroup, DEFAULT_MODE, failureHandlerHook);
    }

    /**
     * Instantiates a new Global transaction scanner.
     * 全局事务扫描器
     * @param applicationId      the application id
     * @param txServiceGroup     the tx service group
     * @param mode               the mode
     * @param failureHandlerHook the failure handler hook
     */
    public GlobalTransactionScanner(String applicationId, String txServiceGroup, int mode,
                                    FailureHandler failureHandlerHook) {
        // 设置顺序
        setOrder(ORDER_NUM);
        setProxyTargetClass(true);
        this.applicationId = applicationId;
        this.txServiceGroup = txServiceGroup;
        this.mode = mode;
        this.failureHandlerHook = failureHandlerHook;
    }

    /**
     * 通过终结钩子来销毁 会将内部维护的所有 dispoable 全部执行 destroy 方法
     */
    @Override
    public void destroy() {
        ShutdownHook.getInstance().destroyAll();
    }

    /**
     * 初始化 客户端对象
     */
    private void initClient() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Initializing Global Transaction Clients ... ");
        }
        // 应用id 不能为空
        if (StringUtils.isNullOrEmpty(applicationId) || StringUtils.isNullOrEmpty(txServiceGroup)) {
            throw new IllegalArgumentException(
                "applicationId: " + applicationId + ", txServiceGroup: " + txServiceGroup);
        }
        //init TM
        // 每个 事务组对应唯一一个client实例对象
        TMClient.init(applicationId, txServiceGroup);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(
                "Transaction Manager Client is initialized. applicationId[" + applicationId + "] txServiceGroup["
                    + txServiceGroup + "]");
        }
        //init RM
        // 初始化RM 对象 同样一个事务组对应一个实例
        RMClient.init(applicationId, txServiceGroup);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(
                "Resource Manager is initialized. applicationId[" + applicationId + "] txServiceGroup[" + txServiceGroup
                    + "]");
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Global Transaction Clients are initialized. ");
        }
        // 注册终结钩子
        registerSpringShutdownHook();

    }

    private void registerSpringShutdownHook() {
        if (applicationContext instanceof ConfigurableApplicationContext) {
            // 注册终结钩子
            ((ConfigurableApplicationContext) applicationContext).registerShutdownHook();
            // 这里移除了自己注册的终结钩子
            ShutdownHook.removeRuntimeShutdownHook();
        }
        // 增加 2个 待关闭的对象
        ShutdownHook.getInstance().addDisposable(TmRpcClient.getInstance(applicationId, txServiceGroup));
        ShutdownHook.getInstance().addDisposable(RmRpcClient.getInstance(applicationId, txServiceGroup));
    }

    /**
     * 包装对象
     * @param bean
     * @param beanName
     * @param cacheKey
     * @return
     */
    @Override
    protected Object wrapIfNecessary(Object bean, String beanName, Object cacheKey) {
        // 如果不允许全局事务 直接返回
        if (disableGlobalTransaction) {
            return bean;
        }
        try {
            synchronized (PROXYED_SET) {
                // 如果该对象已经保存到 代理容器中 也是直接返回
                if (PROXYED_SET.contains(beanName)) {
                    return bean;
                }
                interceptor = null;
                //check TCC proxy
                // 判断对应的 Bean 对象是否是 TCC 对象
                if (TCCBeanParserUtils.isTccAutoProxy(bean, beanName, applicationContext)) {
                    //TCC interceptor， proxy bean of sofa:reference/dubbo:reference, and LocalTCC
                    // 生成TCC 的拦截对象
                    interceptor = new TccActionInterceptor(TCCBeanParserUtils.getRemotingDesc(beanName));
                } else {
                    // 如果不是 TCC 事务 生成全局拦截器
                    Class<?> serviceInterface = SpringProxyUtils.findTargetClass(bean);
                    // 获取该类实现的所有接口
                    Class<?>[] interfacesIfJdk = SpringProxyUtils.findInterfaces(bean);

                    // 如果所有方法都不存在 @GlobalTransaction 注解 返回原对象
                    if (!existsAnnotation(new Class[]{serviceInterface})
                        && !existsAnnotation(interfacesIfJdk)) {
                        return bean;
                    }

                    if (interceptor == null) {
                        // 创建 基于全局事务的拦截器
                        interceptor = new GlobalTransactionalInterceptor(failureHandlerHook);
                    }
                }

                LOGGER.info(
                    "Bean[" + bean.getClass().getName() + "] with name [" + beanName + "] would use interceptor ["
                        + interceptor.getClass().getName() + "]");
                if (!AopUtils.isAopProxy(bean)) {
                    // 包装成代理对象
                    bean = super.wrapIfNecessary(bean, beanName, cacheKey);
                } else {
                    AdvisedSupport advised = SpringProxyUtils.getAdvisedSupport(bean);
                    Advisor[] advisor = buildAdvisors(beanName, getAdvicesAndAdvisorsForBean(null, null, null));
                    for (Advisor avr : advisor) {
                        advised.addAdvisor(0, avr);
                    }
                }
                // 将代理对象添加到缓存中
                PROXYED_SET.add(beanName);
                return bean;
            }
        } catch (Exception exx) {
            throw new RuntimeException(exx);
        }
    }

    private boolean existsAnnotation(Class<?>[] classes) {
        if (classes != null && classes.length > 0) {
            for (Class clazz : classes) {
                if (clazz == null) {
                    continue;
                }
                Method[] methods = clazz.getMethods();
                for (Method method : methods) {
                    GlobalTransactional trxAnno = method.getAnnotation(GlobalTransactional.class);
                    if (trxAnno != null) {
                        return true;
                    }

                    GlobalLock lockAnno = method.getAnnotation(GlobalLock.class);
                    if (lockAnno != null) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private MethodDesc makeMethodDesc(GlobalTransactional anno, Method method) {
        return new MethodDesc(anno, method);
    }

    @Override
    protected Object[] getAdvicesAndAdvisorsForBean(Class beanClass, String beanName, TargetSource customTargetSource)
        throws BeansException {
        return new Object[]{interceptor};
    }

    /**
     * 后置函数 当该对象 初始化成功后 就会开启对应的 client 对象
     */
    @Override
    public void afterPropertiesSet() {
        if (disableGlobalTransaction) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Global transaction is disabled.");
            }
            return;
        }
        initClient();

    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        this.setBeanFactory(applicationContext);
    }

    /**
     * 初始化前调用
     * @param bean
     * @param beanName
     * @return
     * @throws BeansException
     */
    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        // 代表该bean 对象是 dataSource 对象
        if (bean instanceof DataSource && !(bean instanceof DataSourceProxy) && ConfigurationFactory.getInstance().getBoolean(DATASOURCE_AUTOPROXY, false)) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Auto proxy of  [" + beanName + "]");
            }
            // 生成一个代理对象
            DataSourceProxy dataSourceProxy = DataSourceProxyHolder.get().putDataSource((DataSource) bean);
            // 这里 CGLIB 的 Enhancer 对象 这里代表 生成一个方法级别的拦截对象
            return Enhancer.create(bean.getClass(), (org.springframework.cglib.proxy.MethodInterceptor) (o, method, args, methodProxy) -> {
                Method m = BeanUtils.findDeclaredMethod(DataSourceProxy.class, method.getName(), method.getParameterTypes());
                if (null != m) {
                    return m.invoke(dataSourceProxy, args);
                } else {
                    return method.invoke(bean, args);
                }
            });
        }
        return bean;
    }
}
