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

import io.seata.core.context.RootContext;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * The type Abstract connection proxy.
 * 连接到 dataSource 的代理对象
 * @author sharajava
 */
public abstract class AbstractConnectionProxy implements Connection {

    /**
     * The Data source proxy.
     * 使用代理的 datasource 对象
     */
    protected DataSourceProxy dataSourceProxy;

    /**
     * The Target connection.
     * 内部维护的实际连接
     */
    protected Connection targetConnection;

    /**
     * Instantiates a new Abstract connection proxy.
     *
     * @param dataSourceProxy  the data source proxy
     * @param targetConnection the target connection
     */
    public AbstractConnectionProxy(DataSourceProxy dataSourceProxy, Connection targetConnection) {
        this.dataSourceProxy = dataSourceProxy;
        this.targetConnection = targetConnection;
    }

    /**
     * Gets data source proxy.
     *
     * @return the data source proxy
     */
    public DataSourceProxy getDataSourceProxy() {
        return dataSourceProxy;
    }

    /**
     * Gets target connection.
     *
     * @return the target connection
     */
    public Connection getTargetConnection() {
        return targetConnection;
    }

    /**
     * Gets db type.
     *
     * @return the db type
     */
    public String getDbType() {
        return dataSourceProxy.getDbType();
    }

    /**
     * 业务代码会通过这里与jdbc 交互
     * @return
     * @throws SQLException
     */
    @Override
    public Statement createStatement() throws SQLException {
        Statement targetStatement = getTargetConnection().createStatement();
        // 封装会话对象  每个  statement 都会绑定一个sql对象 而这种情况就是 没有设置默认的 sql 不过在执行对应的sql语句时可以通过从外部传入来指定
        return new StatementProxy(this, targetStatement);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        // 该方法由 dataSource 层实现 并返回一个会话对象
        PreparedStatement targetPreparedStatement = getTargetConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        // 代理的已准备的 会话对象
        return new PreparedStatementProxy(this, targetPreparedStatement, sql);
    }

    /**
     * 准备 执行某个sql
     * @param sql
     * @return
     * @throws SQLException
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        // 这里不允许是全局任务
        RootContext.assertNotInGlobalTransaction();
        return targetConnection.prepareCall(sql);
    }

    /**
     * 使用本地语法???
     * @param sql
     * @return
     * @throws SQLException
     */
    @Override
    public String nativeSQL(String sql) throws SQLException {
        return targetConnection.nativeSQL(sql);
    }

    /**
     * 判断是否是自动提交
     * @return
     * @throws SQLException
     */
    @Override
    public boolean getAutoCommit() throws SQLException {
        return targetConnection.getAutoCommit();
    }

    /**
     * 关闭连接
     * @throws SQLException
     */
    @Override
    public void close() throws SQLException {
        targetConnection.close();
    }

    /**
     * 判断连接是否已经被关闭
     * @return
     * @throws SQLException
     */
    @Override
    public boolean isClosed() throws SQLException {
        return targetConnection.isClosed();
    }

    /**
     * 获取数据库的元数据信息
     * @return
     * @throws SQLException
     */
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return targetConnection.getMetaData();
    }

    /**
     * 设置成只读事务
     * @param readOnly
     * @throws SQLException
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        targetConnection.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return targetConnection.isReadOnly();
    }

    /**
     * 设置命名空间 ??? 如果某个 db 不支持该方法 就不会做处理
     * @param catalog
     * @throws SQLException
     */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        targetConnection.setCatalog(catalog);

    }

    @Override
    public String getCatalog() throws SQLException {
        return targetConnection.getCatalog();
    }

    /**
     * 设置事务隔离级别
     * @param level
     * @throws SQLException
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        targetConnection.setTransactionIsolation(level);

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return targetConnection.getTransactionIsolation();
    }

    /**
     * SQLWarning  就是 SQLExeception对象
     * @return
     * @throws SQLException
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return targetConnection.getWarnings();
    }

    /**
     * 清除 异常对象
     * @throws SQLException
     */
    @Override
    public void clearWarnings() throws SQLException {
        targetConnection.clearWarnings();

    }

    /**
     * 创建会话对象
     * @param resultSetType   设置结果集类型
     * @param resultSetConcurrency    代表并发级别 允许 READ-ONLY 和  UPDATE-TABLE
     * @return
     * @throws SQLException
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        Statement statement = targetConnection.createStatement(resultSetType, resultSetConcurrency);
        // 包装会话对象后返回
        return new StatementProxy<Statement>(this, statement);
    }

    /**
     * PreparedStatement 对应着 批量处理
     * @param sql
     * @param resultSetType
     * @param resultSetConcurrency
     * @return
     * @throws SQLException
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        // 创建对应的 批处理会话对象 使用对应的 代理对象进行包装
        PreparedStatement preparedStatement = targetConnection.prepareStatement(sql, resultSetType,
            resultSetConcurrency);
        return new PreparedStatementProxy(this, preparedStatement, sql);
    }

    /**
     * 生成回调对象
     * @param sql
     * @param resultSetType
     * @param resultSetConcurrency
     * @return
     * @throws SQLException
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        RootContext.assertNotInGlobalTransaction();
        return targetConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    /**
     * 获取 类型容器   除非该应用添加了一个实体 否则该方法返回empty
     * @return
     * @throws SQLException
     */
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return targetConnection.getTypeMap();
    }

    /**
     * 主动为 连接设置类型map 对应到上面的方法
     * @param map
     * @throws SQLException
     */
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        targetConnection.setTypeMap(map);

    }

    // 以下方法也是一样 会将方法委托给内部真正的conn 对象

    @Override
    public void setHoldability(int holdability) throws SQLException {
        targetConnection.setHoldability(holdability);

    }

    @Override
    public int getHoldability() throws SQLException {
        return targetConnection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return targetConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return targetConnection.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        targetConnection.rollback(savepoint);

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        targetConnection.releaseSavepoint(savepoint);

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        Statement statement = targetConnection.createStatement(resultSetType, resultSetConcurrency,
            resultSetHoldability);
        return new StatementProxy<Statement>(this, statement);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        PreparedStatement preparedStatement = targetConnection.prepareStatement(sql, resultSetType,
            resultSetConcurrency, resultSetHoldability);
        return new PreparedStatementProxy(this, preparedStatement, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        RootContext.assertNotInGlobalTransaction();
        return targetConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        PreparedStatement preparedStatement = targetConnection.prepareStatement(sql, autoGeneratedKeys);
        return new PreparedStatementProxy(this, preparedStatement, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        PreparedStatement preparedStatement = targetConnection.prepareStatement(sql, columnIndexes);
        return new PreparedStatementProxy(this, preparedStatement, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        PreparedStatement preparedStatement = targetConnection.prepareStatement(sql, columnNames);
        return new PreparedStatementProxy(this, preparedStatement, sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        return targetConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return targetConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return targetConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return targetConnection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return targetConnection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        targetConnection.setClientInfo(name, value);

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        targetConnection.setClientInfo(properties);

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return targetConnection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return targetConnection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return targetConnection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return targetConnection.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        targetConnection.setSchema(schema);

    }

    @Override
    public String getSchema() throws SQLException {
        return targetConnection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        targetConnection.abort(executor);

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        targetConnection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return targetConnection.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return targetConnection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return targetConnection.isWrapperFor(iface);
    }
}
