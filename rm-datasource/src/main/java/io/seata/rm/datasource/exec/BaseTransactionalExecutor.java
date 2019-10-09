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
package io.seata.rm.datasource.exec;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import com.alibaba.druid.util.JdbcConstants;
import io.seata.common.util.CollectionUtils;
import io.seata.common.util.StringUtils;
import io.seata.core.context.RootContext;
import io.seata.rm.datasource.ConnectionProxy;
import io.seata.rm.datasource.ParametersHolder;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.sql.SQLRecognizer;
import io.seata.rm.datasource.sql.SQLType;
import io.seata.rm.datasource.sql.WhereRecognizer;
import io.seata.rm.datasource.sql.struct.Field;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableMetaCache;
import io.seata.rm.datasource.sql.struct.TableMetaCacheOracle;
import io.seata.rm.datasource.sql.struct.TableRecords;
import io.seata.rm.datasource.undo.SQLUndoLog;

/**
 * The type Base transactional executor.
 * 基于事务的 执行器对象
 * @author sharajava
 *
 * @param <T> the type parameter
 * @param <S> the type parameter
 */
public abstract class BaseTransactionalExecutor<T, S extends Statement> implements Executor {

    /**
     * The Statement proxy.
     * 会话代理对象 内部维护了真正的statement 对象
     */
    protected StatementProxy<S> statementProxy;

    /**
     * The Statement callback.
     * 回调对象 实际上 sql 是由该对象执行的
     */
    protected StatementCallback<T, S> statementCallback;

    /**
     * The Sql recognizer.
     * sql解析器对象
     */
    protected SQLRecognizer sqlRecognizer;

    /**
     * 表相关的元数据
     */
    private TableMeta tableMeta;

    /**
     * Instantiates a new Base transactional executor.
     *
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param sqlRecognizer     the sql recognizer
     */
    public BaseTransactionalExecutor(StatementProxy<S> statementProxy, StatementCallback<T, S> statementCallback,
                                     SQLRecognizer sqlRecognizer) {
        this.statementProxy = statementProxy;
        this.statementCallback = statementCallback;
        this.sqlRecognizer = sqlRecognizer;
    }

    /**
     * dao 层的执行 最终会委托到这层
     * @param args the args
     * @return
     * @throws Throwable
     */
    @Override
    public Object execute(Object... args) throws Throwable {
        // 判断当前是否在一个全局事务中
        if (RootContext.inGlobalTransaction()) {
            // 获取全局事务id
            String xid = RootContext.getXID();
            // 将会话绑定到该事务上
            statementProxy.getConnectionProxy().bind(xid);
        }

        // 判断是否需要全局锁
        if (RootContext.requireGlobalLock()) {
            statementProxy.getConnectionProxy().setGlobalLockRequire(true);
        } else {
            statementProxy.getConnectionProxy().setGlobalLockRequire(false);
        }
        // 真正的执行逻辑由子类实现
        return doExecute(args);
    }

    /**
     * Do execute object.
     *
     * @param args the args
     * @return the object
     * @throws Throwable the throwable
     */
    protected abstract Object doExecute(Object... args) throws Throwable;

    /**
     * Build where condition by p ks string.
     * 使用主键来生成where 条件
     * @param pkRows the pk rows
     * @return the string
     * @throws SQLException the sql exception
     */
    protected String buildWhereConditionByPKs(List<Field> pkRows) throws SQLException {
        StringJoiner whereConditionAppender = new StringJoiner(" OR ");
        for (Field field : pkRows) {
            // 会变成 pk1 = ? OR pk2 = ? OR pk3 = ?
            whereConditionAppender.add(getColumnNameInSQL(field.getName()) + " = ?");
        }
        return whereConditionAppender.toString();

    }

    /**
     * build buildWhereCondition
     * 构建where 条件
     * @param recognizer        the recognizer
     * @param paramAppenderList the param paramAppender list
     * @return the string
     */
    protected String buildWhereCondition(WhereRecognizer recognizer, ArrayList<List<Object>> paramAppenderList) {
        String whereCondition = null;
        // 代表 PreparedStatementProxy 对象   并获取 where 的条件表达式
        if (statementProxy instanceof ParametersHolder) {
            whereCondition = recognizer.getWhereCondition((ParametersHolder) statementProxy, paramAppenderList);
        } else {
            whereCondition = recognizer.getWhereCondition();
        }
        //process batch operation
        // 处理批操作
        if (StringUtils.isNotBlank(whereCondition) && CollectionUtils.isNotEmpty(paramAppenderList) && paramAppenderList.size() > 1) {
            StringBuilder whereConditionSb = new StringBuilder();
            whereConditionSb.append(" ( ").append(whereCondition).append(" ) ");
            for (int i = 1; i < paramAppenderList.size(); i++) {
                whereConditionSb.append(" or ( ").append(whereCondition).append(" ) ");
            }
            whereCondition = whereConditionSb.toString();
        }
        return whereCondition;
    }

    /**
     * Gets column name in sql.
     * 从sql 中分解出col 名字
     * @param columnName the column name
     * @return the column name in sql
     */
    protected String getColumnNameInSQL(String columnName) {
        // 这层由datasource 层实现
        String tableAlias = sqlRecognizer.getTableAlias();
        return tableAlias == null ? columnName : tableAlias + "." + columnName;
    }

    /**
     * Gets from table in sql.
     * 获取 表名  底层通过 datasource 那层实现  比如 druid
     * @return the from table in sql
     */
    protected String getFromTableInSQL() {
        String tableName = sqlRecognizer.getTableName();
        String tableAlias = sqlRecognizer.getTableAlias();
        return tableAlias == null ? tableName : tableName + " " + tableAlias;
    }

    /**
     * Gets table meta.
     * 获取元数据信息
     * @return the table meta
     */
    protected TableMeta getTableMeta() {
        return getTableMeta(sqlRecognizer.getTableName());
    }

    /**
     * Gets table meta.
     * 通过表名获取元数据信息
     * @param tableName the table name
     * @return the table meta
     */
    protected TableMeta getTableMeta(String tableName) {
        if (tableMeta != null) {
            return tableMeta;
        }
        if (JdbcConstants.ORACLE.equalsIgnoreCase(statementProxy.getConnectionProxy().getDbType())) {
            tableMeta = TableMetaCacheOracle.getTableMeta(statementProxy.getConnectionProxy().getDataSourceProxy(), tableName);
        } else {
            // 从缓存中获取 如果没有的化就创建新数据 并设置到引用中
            // 生成方式就是通过 connection 去数据库拉取 表的相关信息
            tableMeta = TableMetaCache.getTableMeta(statementProxy.getConnectionProxy().getDataSourceProxy(), tableName);
        }
        return tableMeta;
    }

    /**
     * prepare undo log.
     * 创建撤销日志
     * @param beforeImage the before image   代表之前的快照信息
     * @param afterImage  the after image    之后的快照信息
     * @throws SQLException the sql exception
     */
    protected void prepareUndoLog(TableRecords beforeImage, TableRecords afterImage) throws SQLException {
        if (beforeImage.getRows().size() == 0 && afterImage.getRows().size() == 0) {
            return;
        }

        ConnectionProxy connectionProxy = statementProxy.getConnectionProxy();

        // 这里什么意思 如果是DELETE 类型就返回before 否则 返回after???
        TableRecords lockKeyRecords = sqlRecognizer.getSQLType() == SQLType.DELETE ? beforeImage : afterImage;
        // 构建锁语句
        String lockKeys = buildLockKey(lockKeyRecords);
        // 这里是设置到 connectionContext 的 一个 buffer 中(一个set结构)
        connectionProxy.appendLockKey(lockKeys);

        // 通过 before 和after 对象生成 一个 SQLUndoLog 对象
        SQLUndoLog sqlUndoLog = buildUndoItem(beforeImage, afterImage);
        // 保存到上下文中 也是保存到某个set中
        connectionProxy.appendUndoLog(sqlUndoLog);
    }

    /**
     * build lockKey
     * 构建锁语句   返回的应该是 tableName:pk1,pk2....
     * @param rowsIncludingPK the records
     * @return the string
     */
    protected String buildLockKey(TableRecords rowsIncludingPK) {
        if (rowsIncludingPK.size() == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        // 表名
        sb.append(rowsIncludingPK.getTableMeta().getTableName());
        sb.append(":");
        int filedSequence = 0;
        // 主键名
        for (Field field : rowsIncludingPK.pkRows()) {
            sb.append(field.getValue());
            filedSequence++;
            if (filedSequence < rowsIncludingPK.pkRows().size()) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    /**
     * build a SQLUndoLog
     * 构建撤销日志
     * @param beforeImage the before image
     * @param afterImage  the after image
     * @return sql undo log
     */
    protected SQLUndoLog buildUndoItem(TableRecords beforeImage, TableRecords afterImage) {
        SQLType sqlType = sqlRecognizer.getSQLType();
        String tableName = sqlRecognizer.getTableName();

        SQLUndoLog sqlUndoLog = new SQLUndoLog();
        sqlUndoLog.setSqlType(sqlType);
        sqlUndoLog.setTableName(tableName);
        sqlUndoLog.setBeforeImage(beforeImage);
        sqlUndoLog.setAfterImage(afterImage);
        return sqlUndoLog;
    }


    /**
     * build a BeforeImage
     * 构建 tableRecord 信息
     * @param tableMeta         the tableMeta
     * @param selectSQL         the selectSQL
     * @param paramAppenderList the paramAppender list
     * @return a tableRecords
     * @throws SQLException the sql exception
     */
    protected TableRecords buildTableRecords(TableMeta tableMeta, String selectSQL, ArrayList<List<Object>> paramAppenderList) throws SQLException {
        TableRecords tableRecords = null;
        PreparedStatement ps = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            // 当不存在参数时 就创建 普通的 statement 对象
            if (paramAppenderList.isEmpty()) {
                // 通过 java.sql.connection 对象 构建statement对象
                st = statementProxy.getConnection().createStatement();
                // 执行查询语句
                rs = st.executeQuery(selectSQL);
            // 创建携带参数的 preparedStatement 对象
            } else {
                if (paramAppenderList.size() == 1) {
                    ps = statementProxy.getConnection().prepareStatement(selectSQL);
                    List<Object> paramAppender = paramAppenderList.get(0);
                    for (int i = 0; i < paramAppender.size(); i++) {
                        ps.setObject(i + 1, paramAppender.get(i));
                    }
                } else {
                    // 如果 paramAppenderList 本身长度不为1  有这么多参数吗???
                    ps = statementProxy.getConnection().prepareStatement(selectSQL);
                    List<Object> paramAppender = null;
                    for (int i = 0; i < paramAppenderList.size(); i++) {
                        paramAppender = paramAppenderList.get(i);
                        for (int j = 0; j < paramAppender.size(); j++) {
                            ps.setObject(i * paramAppender.size() + j + 1, paramAppender.get(j));
                        }
                    }
                }
                rs = ps.executeQuery();
            }
            // 通过结果集 和 元数据信息生成 tableRecord 对象
            tableRecords = TableRecords.buildRecords(tableMeta, rs);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }
            if (ps != null) {
                ps.close();
            }
        }
        return tableRecords;
    }

    /**
     * build TableRecords
     * 通过一组主键对象来构建 tableRecord
     * @param pkValues the pkValues
     * @return return TableRecords;
     * @throws SQLException
     */
    protected TableRecords buildTableRecords(List<Object> pkValues) throws SQLException {
        TableRecords afterImage;
        String pk = getTableMeta().getPkName();
        StringJoiner pkValuesJoiner = new StringJoiner(" OR ", "SELECT * FROM " + getTableMeta().getTableName() + " WHERE ", "");
        for (Object pkValue : pkValues) {
            pkValuesJoiner.add(pk + "=?");
        }
        PreparedStatement ps = null;
        ResultSet rs = null;
        // 生成一个 依赖一组pk 对象进行查询的 select 语句
        try {
            ps = statementProxy.getConnection().prepareStatement(pkValuesJoiner.toString());

            for (int i = 1; i <= pkValues.size(); i++) {
                ps.setObject(i, pkValues.get(i - 1));
            }

            // 查询并生成结果
            rs = ps.executeQuery();
            // 返回after 对象 可以理解为一个新快照吗???
            afterImage = TableRecords.buildRecords(getTableMeta(), rs);

        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
        }
        return afterImage;
    }

}
