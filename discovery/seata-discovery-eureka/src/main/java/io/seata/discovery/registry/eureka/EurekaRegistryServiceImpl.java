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
package io.seata.discovery.registry.eureka;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.providers.EurekaConfigBasedInstanceInfoProvider;
import com.netflix.config.ConfigurationManager;
import com.netflix.discovery.DefaultEurekaClientConfig;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaEvent;
import com.netflix.discovery.EurekaEventListener;
import com.netflix.discovery.shared.Application;
import io.seata.common.exception.EurekaRegistryException;
import io.seata.common.util.CollectionUtils;
import io.seata.common.util.NetUtil;
import io.seata.common.util.StringUtils;
import io.seata.config.Configuration;
import io.seata.config.ConfigurationFactory;
import io.seata.discovery.registry.RegistryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The type Eureka registry service.
 * 获取 eureka 注册中心实例对象 实现了 RegistryService 接口
 * @author: rui_849217@163.com
 * @date: 2018/3/6
 */
public class EurekaRegistryServiceImpl implements RegistryService<EurekaEventListener> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EurekaRegistryServiceImpl.class);

    private static final String DEFAULT_APPLICATION = "default";
    private static final String PRO_SERVICE_URL_KEY = "serviceUrl";
    private static final String FILE_ROOT_REGISTRY = "registry";
    private static final String FILE_CONFIG_SPLIT_CHAR = ".";
    private static final String REGISTRY_TYPE = "eureka";
    private static final String CLUSTER = "application";
    private static final String REGISTRY_WEIGHT = "weight";
    private static final String EUREKA_CONFIG_SERVER_URL_KEY = "eureka.serviceUrl.default";
    private static final String EUREKA_CONFIG_REFRESH_KEY = "eureka.client.refresh.interval";
    private static final String EUREKA_CONFIG_SHOULD_REGISTER = "eureka.registration.enabled";
    private static final String EUREKA_CONFIG_METADATA_WEIGHT = "eureka.metadata.weight";
    private static final int EUREKA_REFRESH_INTERVAL = 5;
    private static final int MAP_INITIAL_CAPACITY = 8;
    private static final String DEFAULT_WEIGHT = "1";
    private static final Configuration FILE_CONFIG = ConfigurationFactory.CURRENT_FILE_INSTANCE;

    /**
     * 集群容器
     */
    private static ConcurrentMap<String, Set<InetSocketAddress>> clusterAddressMap;

    private static volatile boolean subscribeListener = false;

    /**
     * 内部包含监听器对象
     */
    private static volatile ApplicationInfoManager applicationInfoManager;
    /**
     * 使用允许自定义 appId address 等的config 对象
     */
    private static volatile CustomEurekaInstanceConfig instanceConfig;
    private static volatile EurekaRegistryServiceImpl instance;
    /**
     * 该对象对应到 eureka 的client 对象 该对象会从配置中心 拉取服务端地址 并且会在初始化后将自身注册到上面,还具备从注册中心拉取服务列表的能力
     */
    private static volatile EurekaClient eurekaClient;


    private EurekaRegistryServiceImpl() {
    }

    static EurekaRegistryServiceImpl getInstance() {
        if (null == instance) {
            synchronized (EurekaRegistryServiceImpl.class) {
                if (null == instance) {
                    clusterAddressMap = new ConcurrentHashMap<>(MAP_INITIAL_CAPACITY);
                    instanceConfig = new CustomEurekaInstanceConfig();
                    instance = new EurekaRegistryServiceImpl();
                }
            }
        }
        return instance;
    }

    /**
     * 将某个地址设置到注册中心
     * @param address the address
     * @throws Exception
     */
    @Override
    public void register(InetSocketAddress address) throws Exception {
        NetUtil.validAddress(address);
        instanceConfig.setIpAddress(address.getAddress().getHostAddress());
        instanceConfig.setPort(address.getPort());
        instanceConfig.setApplicationName(getApplicationName());
        instanceConfig.setInstanceId(getInstanceId());
        // 做初始化工作
        getEurekaClient(true);
        // 触发内部的监听器
        applicationInfoManager.setInstanceStatus(InstanceInfo.InstanceStatus.UP);
    }

    @Override
    public void unregister(InetSocketAddress address) throws Exception {
        if (eurekaClient == null) {
            return;
        }
        applicationInfoManager.setInstanceStatus(InstanceInfo.InstanceStatus.DOWN);
    }

    @Override
    public void subscribe(String cluster, EurekaEventListener listener) throws Exception {
        subscribeListener = true;
        getEurekaClient(false).registerEventListener(listener);
    }

    @Override
    public void unsubscribe(String cluster, EurekaEventListener listener) throws Exception {
        subscribeListener = false;
        getEurekaClient(false).unregisterEventListener(listener);
    }

    /**
     * 查询一组服务实例的地址
     * @param key the key
     * @return
     * @throws Exception
     */
    @Override
    public List<InetSocketAddress> lookup(String key) throws Exception {
        Configuration config = ConfigurationFactory.getInstance();
        String clusterName = config.getConfig(PREFIX_SERVICE_ROOT + CONFIG_SPLIT_CHAR + PREFIX_SERVICE_MAPPING + key);
        if (null == clusterName) {
            return null;
        }
        // 这里会设置一个 刷新集群信息的监听器
        if (!subscribeListener) {
            refreshCluster();
            subscribe(null, new EurekaEventListener() {
                @Override
                public void onEvent(EurekaEvent event) {
                    try {
                        refreshCluster();
                    } catch (Exception e) {
                        LOGGER.error("Eureka event listener refreshCluster error:{}", e.getMessage(), e);
                    }
                }
            });
        }

        // 集群地址应该是存放在clusterAddressMap 中
        return new ArrayList<>(clusterAddressMap.get(clusterName.toUpperCase()));
    }

    @Override
    public void close() throws Exception {
        if (eurekaClient != null) {
            eurekaClient.shutdown();
        }
        clean();
    }

    /**
     * 刷新集群信息
     */
    private void refreshCluster() {
        // 就是从注册中心获取当前所有应用  同时因为注册中心本身是定时更新列表的 理想情况下 该信息比较准确
        List<Application> applications = getEurekaClient(false).getApplications().getRegisteredApplications();

        if (CollectionUtils.isEmpty(applications)){
            // 代表 应用都失效了 就清空列表
            clusterAddressMap.clear();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("refreshCluster success, cluster empty!");
            }
            return;
        }

        ConcurrentMap<String, Set<InetSocketAddress>> collect = new ConcurrentHashMap<>(MAP_INITIAL_CAPACITY);

        for (Application application : applications) {
            List<InstanceInfo> instances = application.getInstances();

            if (CollectionUtils.isNotEmpty(instances)) {
                Set<InetSocketAddress> addressSet = new HashSet<>();
                for (InstanceInfo instance : instances) {
                    addressSet.add(new InetSocketAddress(instance.getIPAddr(), instance.getPort()));
                }
                // 以应用名为单位 同一个应用名的属于同一个集群  这里的应用名就是 事务组名 server 以事务组为单位在注册中心进行注册
                collect.put(application.getName(), addressSet);
            }
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("refreshCluster success, cluster: " + collect);
        }

        clusterAddressMap = collect;
    }

    /**
     * 获取eureka 配置信息
     * @param needRegister 这里的注册是指是否将本实例注册到注册中心上
     * @return
     */
    private Properties getEurekaProperties(boolean needRegister) {
        Properties eurekaProperties = new Properties();
        eurekaProperties.setProperty(EUREKA_CONFIG_REFRESH_KEY, String.valueOf(EUREKA_REFRESH_INTERVAL));

        String url = FILE_CONFIG.getConfig(getEurekaServerUrlFileKey());
        if (StringUtils.isBlank(url)) {
            throw new EurekaRegistryException("eureka server url can not be null!");
        }
        eurekaProperties.setProperty(EUREKA_CONFIG_SERVER_URL_KEY, url);

        String weight = FILE_CONFIG.getConfig(getEurekaInstanceWeightFileKey());
        if (StringUtils.isNotBlank(weight)) {
            eurekaProperties.setProperty(EUREKA_CONFIG_METADATA_WEIGHT, weight);
        } else {
            eurekaProperties.setProperty(EUREKA_CONFIG_METADATA_WEIGHT, DEFAULT_WEIGHT);
        }

        if (!needRegister) {
            eurekaProperties.setProperty(EUREKA_CONFIG_SHOULD_REGISTER, "false");
        }

        return eurekaProperties;
    }

    private String getApplicationName() {
        String application = FILE_CONFIG.getConfig(getEurekaApplicationFileKey());
        if (null == application) {
            application = DEFAULT_APPLICATION;
        }
        return application;
    }

    /**
     * 获取 eurekaClient 对象
     * @param needRegister
     * @return
     * @throws EurekaRegistryException
     */
    private EurekaClient getEurekaClient(boolean needRegister) throws EurekaRegistryException {
        if (eurekaClient == null) {
            synchronized (EurekaRegistryServiceImpl.class) {
                try {
                    if (!needRegister) {
                        instanceConfig = new CustomEurekaInstanceConfig();
                    }
                    // 将prop 的配置信息 设置到 ConfManager上
                    ConfigurationManager.loadProperties(getEurekaProperties(needRegister));
                    // 使用指定的配置去生成实例对象(注册中心的注册实例单位)
                    InstanceInfo instanceInfo = new EurekaConfigBasedInstanceInfoProvider(instanceConfig).get();
                    applicationInfoManager = new ApplicationInfoManager(instanceConfig, instanceInfo);
                    // 将 AppManager 注册到client 上以便监听服务实例的上下线
                    eurekaClient = new DiscoveryClient(applicationInfoManager, new DefaultEurekaClientConfig());
                } catch (Exception e) {
                    clean();
                    throw new EurekaRegistryException("register eureka is error!", e);
                }
            }
        }
        return eurekaClient;
    }

    private void clean() {
        eurekaClient = null;
        applicationInfoManager = null;
        instanceConfig = null;
    }

    private String getInstanceId() {
        return String.format("%s:%s:%d", instanceConfig.getAppname(), instanceConfig.getIpAddress(),
            instanceConfig.getNonSecurePort());
    }

    private String getEurekaServerUrlFileKey() {
        return FILE_ROOT_REGISTRY + FILE_CONFIG_SPLIT_CHAR + REGISTRY_TYPE + FILE_CONFIG_SPLIT_CHAR
            + PRO_SERVICE_URL_KEY;
    }

    private String getEurekaApplicationFileKey() {
        return FILE_ROOT_REGISTRY + FILE_CONFIG_SPLIT_CHAR + REGISTRY_TYPE + FILE_CONFIG_SPLIT_CHAR
            + CLUSTER;
    }

    private String getEurekaInstanceWeightFileKey() {
        return FILE_ROOT_REGISTRY + FILE_CONFIG_SPLIT_CHAR + REGISTRY_TYPE + FILE_CONFIG_SPLIT_CHAR
            + REGISTRY_WEIGHT;
    }

}
