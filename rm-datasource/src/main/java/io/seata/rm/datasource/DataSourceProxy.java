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
package io.seata.rm.datasource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import com.alibaba.druid.util.JdbcUtils;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.model.BranchType;
import io.seata.core.model.Resource;
import io.seata.rm.DefaultResourceManager;
import io.seata.rm.datasource.sql.struct.TableMetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Data source proxy.
 * 数据源代理对象   该对象被看作一个 resource 可以注册到RM 上
 * 在spring 框架下 所有通过dataSource 的操作都会被委托到这层  (在scan那里做了拦截)
 * @author sharajava
 */
public class DataSourceProxy extends AbstractDataSourceProxy implements Resource {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceProxy.class);

    /**
     * 资源组id 默认是 DEFAULT
     */
    private String resourceGroupId;

    private static final String DEFAULT_RESOURCE_GROUP_ID = "DEFAULT";

    /**
     * jdbc 连接地址
     */
    private String jdbcUrl;

    /**
     * 数据库类型
     */
    private String dbType;

    /**
     * Enable the table meta checker
     * 是否检查表的元数据信息
     */
    private static boolean ENABLE_TABLE_META_CHECKER_ENABLE = ConfigurationFactory.getInstance().getBoolean(ConfigurationKeys.CLIENT_TABLE_META_CHECK_ENABLE, true);

    /**
     * Table meta checker interval
     */
    private static final long TABLE_META_CHECKER_INTERVAL = 60000L;

    /**
     * 内部包含一个线程池
     */
    private final ScheduledExecutorService tableMetaExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("tableMetaChecker", 1, true));

    /**
     * Instantiates a new Data source proxy.
     * 将一个普通的datasource 对象包装成一个代理对象 在没有设置资源组的情况 下使用默认的资源组
     * @param targetDataSource the target data source
     */
    public DataSourceProxy(DataSource targetDataSource) {
        this(targetDataSource, DEFAULT_RESOURCE_GROUP_ID);
    }

    /**
     * Instantiates a new Data source proxy.
     * 通过datasource 来初始化代理对象
     * @param targetDataSource the target data source
     * @param resourceGroupId  the resource group id
     */
    public DataSourceProxy(DataSource targetDataSource, String resourceGroupId) {
        super(targetDataSource);
        // 进行初始化
        init(targetDataSource, resourceGroupId);
    }

    /**
     * 在创建datasourceProxy 对象后 进行初始化
     * @param dataSource
     * @param resourceGroupId
     */
    private void init(DataSource dataSource, String resourceGroupId) {
        this.resourceGroupId = resourceGroupId;
        // 获取连接对象
        try (Connection connection = dataSource.getConnection()) {
            jdbcUrl = connection.getMetaData().getURL();
            // 从jdbcUrl 上解析 db类型
            dbType = JdbcUtils.getDbType(jdbcUrl, null);
        } catch (SQLException e) {
            throw new IllegalStateException("can not init dataSource", e);
        }
        // 将自身信息注册到 RM 上
        DefaultResourceManager.get().registerResource(this);
        // 如果开启了 tableMeta 检查  定期刷新 元数据信息
        if(ENABLE_TABLE_META_CHECKER_ENABLE){
            tableMetaExecutor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    TableMetaCache.refresh(DataSourceProxy.this);
                }
            }, 0, TABLE_META_CHECKER_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Gets plain connection.
     *
     * @return the plain connection
     * @throws SQLException the sql exception
     */
    public Connection getPlainConnection() throws SQLException {
        return targetDataSource.getConnection();
    }

    /**
     * Gets db type.
     *
     * @return the db type
     */
    public String getDbType() {
        return dbType;
    }

    /**
     * 原本 spring 执行jdbc 的过程就是 获取 datasource -> 获取 connection -> 获取statement -> 开始与jdbc交互生成resultSet
     * @return
     * @throws SQLException
     */
    @Override
    public ConnectionProxy getConnection() throws SQLException {
        Connection targetConnection = targetDataSource.getConnection();
        return new ConnectionProxy(this, targetConnection);
    }

    @Override
    public ConnectionProxy getConnection(String username, String password) throws SQLException {
        Connection targetConnection = targetDataSource.getConnection(username, password);
        return new ConnectionProxy(this, targetConnection);
    }

    @Override
    public String getResourceGroupId() {
        return resourceGroupId;
    }

    /**
     * 获取资源id 与 jdbcUrl 关联
     * @return
     */
    @Override
    public String getResourceId() {
        if (jdbcUrl.contains("?")) {
            return jdbcUrl.substring(0, jdbcUrl.indexOf("?"));
        } else {
            return jdbcUrl;
        }
    }

    /**
     * 基于datasource 代理的是 AT 模式 也就是自动模式
     * @return
     */
    @Override
    public BranchType getBranchType() {
        return BranchType.AT;
    }
}
