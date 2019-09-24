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
package io.seata.common.loader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.seata.common.Constants;
import io.seata.common.executor.Initialize;
import io.seata.common.util.CollectionUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Enhanced service loader.
 * 增强的 SPI
 *
 * @author jimin.jm @alibaba-inc.com
 * @date 2018 /10/10
 */
public class EnhancedServiceLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(EnhancedServiceLoader.class);

    // 指定SPI 文件放置位置

    private static final String SERVICES_DIRECTORY = "META-INF/services/";
    private static final String SEATA_DIRECTORY = "META-INF/seata/";

    /**
     * 静态全局对象  该对象记录了 Service类 与 下面所有实现类的映射关系
     */
    @SuppressWarnings("rawtypes")
    private static Map<Class, List<Class>> providers = new ConcurrentHashMap<Class, List<Class>>();

    /**
     * Specify classLoader to load the service provider
     *
     * @param <S>     the type parameter
     * @param service the service
     * @param loader  the loader
     * @return s s
     * @throws EnhancedServiceNotFoundException the enhanced service not found exception
     * 根据 指定的 service 类型从文件中加载实现类
     */
    public static <S> S load(Class<S> service, ClassLoader loader) throws EnhancedServiceNotFoundException {
        return loadFile(service, null, loader);
    }

    /**
     * load service provider
     * 使用加载该对象相同的类加载器解析 SPI文件
     *
     * @param <S>     the type parameter
     * @param service the service
     * @return s s
     * @throws EnhancedServiceNotFoundException the enhanced service not found exception
     */
    public static <S> S load(Class<S> service) throws EnhancedServiceNotFoundException {
        return loadFile(service, null, findClassLoader());
    }

    /**
     * load service provider
     *
     * @param <S>          the type parameter
     * @param service      the service
     * @param activateName the activate name
     * @return s s
     * @throws EnhancedServiceNotFoundException the enhanced service not found exception
     */
    public static <S> S load(Class<S> service, String activateName) throws EnhancedServiceNotFoundException {
        return loadFile(service, activateName, findClassLoader());
    }

    /**
     * Specify classLoader to load the service provider
     *
     * @param <S>          the type parameter
     * @param service      the service
     * @param activateName the activate name
     * @param loader       the loader
     * @return s s
     * @throws EnhancedServiceNotFoundException the enhanced service not found exception
     */
    public static <S> S load(Class<S> service, String activateName, ClassLoader loader)
        throws EnhancedServiceNotFoundException {
        return loadFile(service, activateName, loader);
    }

    /**
     * Load s.
     * 静态方法 通过传入的参数 初始化对象
     *
     * @param <S>          the type parameter
     * @param service      the service
     * @param activateName the activate name
     * @param args         the args
     * @return the s
     * @throws EnhancedServiceNotFoundException the enhanced service not found exception
     */
    public static <S> S load(Class<S> service, String activateName, Object[] args)
        throws EnhancedServiceNotFoundException {
        // 生成参数类型列表 便于匹配实现类的构造函数
        Class[] argsType = null;
        if (args != null && args.length > 0) {
            argsType = new Class[args.length];
            for (int i = 0; i < args.length; i++) {
                argsType[i] = args[i].getClass();
            }
        }
        // 使用指定构造函数 和参数来进行初始化
        return loadFile(service, activateName, findClassLoader(), argsType, args);
    }

    /**
     * Load s.
     *
     * @param <S>          the type parameter
     * @param service      the service
     * @param activateName the activate name
     * @param argsType     the args type
     * @param args         the args
     * @return the s
     * @throws EnhancedServiceNotFoundException the enhanced service not found exception
     */
    public static <S> S load(Class<S> service, String activateName, Class[] argsType, Object[] args)
        throws EnhancedServiceNotFoundException {
        return loadFile(service, activateName, findClassLoader(), argsType, args);
    }

    /**
     * get all implements
     * 加载所有实现
     * @param <S>     the type parameter
     * @param service the service
     * @return list list
     */
    public static <S> List<S> loadAll(Class<S> service) {
        List<S> allInstances = new ArrayList<>();
        // 加载所有SPI 实现类
        List<Class> allClazzs = getAllExtensionClass(service);
        if (CollectionUtils.isEmpty(allClazzs)) {
            return allInstances;
        }
        try {
            for (Class clazz : allClazzs) {
                allInstances.add(initInstance(service, clazz, null, null));
            }
        } catch (Throwable t) {
            throw new EnhancedServiceNotFoundException(t);
        }
        return allInstances;
    }

    /**
     * Get all the extension classes, follow {@linkplain LoadLevel} defined and sort order
     * 加载所有SPI 实现类
     *
     * @param <S>     the type parameter
     * @param service the service
     * @return all extension class
     */
    @SuppressWarnings("rawtypes")
    public static <S> List<Class> getAllExtensionClass(Class<S> service) {
        return findAllExtensionClass(service, null, findClassLoader());
    }

    /**
     * Get all the extension classes, follow {@linkplain LoadLevel} defined and sort order
     *
     * @param <S>     the type parameter
     * @param service the service
     * @param loader  the loader
     * @return all extension class
     */
    @SuppressWarnings("rawtypes")
    public static <S> List<Class> getAllExtensionClass(Class<S> service, ClassLoader loader) {
        return findAllExtensionClass(service, null, loader);
    }

    /**
     * 加载SPI 配置文件
     * @param service
     * @param activateName  代表哪个被激活
     * @param loader
     * @param <S>
     * @return
     */
    private static <S> S loadFile(Class<S> service, String activateName, ClassLoader loader) {
        return loadFile(service, activateName, loader, null, null);
    }

    /**
     * 从 SPI 配置文件中加载 某个服务的实现类 并使用指定参数进行初始化
     * @param service
     * @param activateName
     * @param loader
     * @param argTypes
     * @param args
     * @param <S>
     * @return
     */
    @SuppressWarnings("rawtypes")
    private static <S> S loadFile(Class<S> service, String activateName, ClassLoader loader, Class[] argTypes,
                                  Object[] args) {
        try {
            // 是否从缓存中获取
            boolean foundFromCache = true;
            // 尝试从缓存获取
            List<Class> extensions = providers.get(service);
            if (extensions == null) {
                synchronized (service) {
                    // 难怪要用 ConcurrentHashMap
                    extensions = providers.get(service);
                    if (extensions == null) {
                        // 加载所有的 实现类
                        extensions = findAllExtensionClass(service, activateName, loader);
                        foundFromCache = false;
                        providers.put(service, extensions);
                    }
                }
            }
            // 寻找要被激活的实现类
            if (StringUtils.isNotEmpty(activateName)) {
                // 这里多套了一层目录结构 用来限定范围的 这种文件下一般只设置一个 实现类
                loadFile(service, SEATA_DIRECTORY + activateName.toLowerCase() + "/", loader, extensions);

                List<Class> activateExtensions = new ArrayList<Class>();
                for (int i = 0; i < extensions.size(); i++) {
                    Class clz = extensions.get(i);
                    @SuppressWarnings("unchecked")
                    LoadLevel activate = (LoadLevel)clz.getAnnotation(LoadLevel.class);
                    if (activate != null && activateName.equalsIgnoreCase(activate.name())) {
                        activateExtensions.add(clz);
                    }
                }

                extensions = activateExtensions;
            }

            // 没有实现类 抛出异常
            if (extensions.isEmpty()) {
                throw new EnhancedServiceNotFoundException(
                    "not found service provider for : " + service.getName() + "[" + activateName
                        + "] and classloader : " + ObjectUtils.toString(loader));
            }
            // 获取优先级最低的
            Class<?> extension = extensions.get(extensions.size() - 1);
            S result = initInstance(service, extension, argTypes, args);
            if (!foundFromCache && LOGGER.isInfoEnabled()) {
                LOGGER.info("load " + service.getSimpleName() + "[" + activateName + "] extension by class[" + extension
                    .getName() + "]");
            }
            return result;
        } catch (Throwable e) {
            if (e instanceof EnhancedServiceNotFoundException) {
                throw (EnhancedServiceNotFoundException)e;
            } else {
                throw new EnhancedServiceNotFoundException(
                    "not found service provider for : " + service.getName() + " caused by " + ExceptionUtils
                        .getFullStackTrace(e));
            }
        }
    }

    /**
     * 从 SPI 中寻找指定service 的全部实现类
     * @param service
     * @param activateName
     * @param loader
     * @param <S>
     * @return
     */
    @SuppressWarnings("rawtypes")
    private static <S> List<Class> findAllExtensionClass(Class<S> service, String activateName, ClassLoader loader) {
        List<Class> extensions = new ArrayList<Class>();
        try {
            // 尝试从 JDK 原生目录 和拓展目录中加载
            loadFile(service, SERVICES_DIRECTORY, loader, extensions);
            loadFile(service, SEATA_DIRECTORY, loader, extensions);
        } catch (IOException e) {
            throw new EnhancedServiceNotFoundException(e);
        }

        if (extensions.isEmpty()) {
            return extensions;
        }
        // 排序后返回 这里利用实现类的 加载级别
        Collections.sort(extensions, new Comparator<Class>() {
            @Override
            public int compare(Class c1, Class c2) {
                Integer o1 = 0;
                Integer o2 = 0;
                @SuppressWarnings("unchecked")
                LoadLevel a1 = (LoadLevel)c1.getAnnotation(LoadLevel.class);
                @SuppressWarnings("unchecked")
                LoadLevel a2 = (LoadLevel)c2.getAnnotation(LoadLevel.class);

                if (a1 != null) {
                    o1 = a1.order();
                }

                if (a2 != null) {
                    o2 = a2.order();
                }

                return o1.compareTo(o2);

            }
        });

        return extensions;
    }

    /**
     * 从指定目录下加载 SPI 实现类
     * @param service
     * @param dir
     * @param classLoader
     * @param extensions
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    private static void loadFile(Class<?> service, String dir, ClassLoader classLoader, List<Class> extensions)
        throws IOException {
        // 生成文件名
        String fileName = dir + service.getName();
        Enumeration<URL> urls;
        // 加载资源
        if (classLoader != null) {
            urls = classLoader.getResources(fileName);
        } else {
            urls = ClassLoader.getSystemResources(fileName);
        }

        if (urls != null) {
            while (urls.hasMoreElements()) {
                // 按行解析
                java.net.URL url = urls.nextElement();
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new InputStreamReader(url.openStream(), Constants.DEFAULT_CHARSET));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        // 如果携带 '#' 号
                        final int ci = line.indexOf('#');
                        // 去除# 的部分
                        if (ci >= 0) {
                            line = line.substring(0, ci);
                        }
                        line = line.trim();
                        if (line.length() > 0) {
                            try {
                                // 加载类 并保存
                                extensions.add(Class.forName(line, true, classLoader));
                            } catch (LinkageError | ClassNotFoundException e) {
                                LOGGER.warn("load [{}] class fail. {}", line, e.getMessage());
                            }
                        }
                    }
                } catch (Throwable e) {
                    LOGGER.warn(e.getMessage());
                } finally {
                    try {
                        if (reader != null) {
                            reader.close();
                        }
                    } catch (IOException ioe) {
                    }
                }
            }
        }
    }

    /**
     * init instance
     *
     * @param <S>       the type parameter
     * @param service   the service
     * @param implClazz the impl clazz
     * @param argTypes  the arg types
     * @param args      the args
     * @return s s
     * @throws IllegalAccessException the illegal access exception
     * @throws InstantiationException the instantiation exception
     * @throws NoSuchMethodException the no such method exception
     * @throws InvocationTargetException the invocation target exception
     * 实例化对象
     */
    protected static <S> S initInstance(Class<S> service, Class implClazz, Class[] argTypes, Object[] args)
        throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        S s = null;
        // 使用指定类型进行初始化
        if (argTypes != null && args != null) {
            // Constructor with arguments
            Constructor<S> constructor = implClazz.getDeclaredConstructor(argTypes);
            constructor.setAccessible(true);
            // 强转
            s = service.cast(constructor.newInstance(args));
        } else {
            // default Constructor
            s = service.cast(implClazz.newInstance());
        }
        // 注意这个接口
        if (s instanceof Initialize) {
            ((Initialize)s).init();
        }
        return s;
    }

    /**
     * Cannot use TCCL, in the pandora container will cause the class in the plugin not to be loaded
     *
     * @return
     */
    private static ClassLoader findClassLoader() {
        return EnhancedServiceLoader.class.getClassLoader();
    }
}
