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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.seata.common.exception.NotSupportYetException;
import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.rm.datasource.PreparedStatementProxy;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.sql.SQLInsertRecognizer;
import io.seata.rm.datasource.sql.SQLRecognizer;
import io.seata.rm.datasource.sql.struct.ColumnMeta;
import io.seata.rm.datasource.sql.struct.Null;
import io.seata.rm.datasource.sql.struct.SqlMethodExpr;
import io.seata.rm.datasource.sql.struct.SqlSequenceExpr;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Insert executor.
 * 针对插入语句的执行器
 * @param <T> the type parameter
 * @param <S> the type parameter
 * @author yuanguoyao
 * @date 2019-03-21 21:30:02
 */
public class InsertExecutor<T, S extends Statement> extends AbstractDMLBaseExecutor<T, S> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertExecutor.class);
    protected static final String ERR_SQL_STATE = "S1009";

    private static final String PLACEHOLDER = "?";

    /**
     * Instantiates a new Insert executor.
     *
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param sqlRecognizer     the sql recognizer
     */
    public InsertExecutor(StatementProxy statementProxy, StatementCallback statementCallback,
                          SQLRecognizer sqlRecognizer) {
        super(statementProxy, statementCallback, sqlRecognizer);
    }

    /**
     * before 对应的是一个空对象 而delete 中after 为空对象 before 为 for update 语句执行后的结果对象
     * @return
     * @throws SQLException
     */
    @Override
    protected TableRecords beforeImage() throws SQLException {
        return TableRecords.empty(getTableMeta());
    }

    /**
     * 后置对象
     * @param beforeImage the before image
     * @return
     * @throws SQLException
     */
    @Override
    protected TableRecords afterImage(TableRecords beforeImage) throws SQLException {
        //Pk column exists or PK is just auto generated
        // 如果包含主键 就获取值 否则获取自动增加的主键的值
        List<Object> pkValues = containsPK() ? getPkValuesByColumn() :
                (containsColumns() ? getPkValuesByAuto() : getPkValuesByColumn());

        TableRecords afterImage = buildTableRecords(pkValues);

        if (afterImage == null) {
            throw new SQLException("Failed to build after-image for insert");
        }

        return afterImage;
    }

    /**
     * 判断是否包含 pk 语句
     * @return
     */
    protected boolean containsPK() {
        SQLInsertRecognizer recognizer = (SQLInsertRecognizer) sqlRecognizer;
        // 获取所有插入col
        List<String> insertColumns = recognizer.getInsertColumns();
        // 获取表的元数据信息 判断是否包含了 pk 字段
        TableMeta tmeta = getTableMeta();
        return tmeta.containsPK(insertColumns);
    }

    /**
     * 是否包含 插入  col
     * @return
     */
    protected boolean containsColumns() {
        SQLInsertRecognizer recognizer = (SQLInsertRecognizer) sqlRecognizer;
        List<String> insertColumns = recognizer.getInsertColumns();
        return insertColumns != null && !insertColumns.isEmpty();
    }

    /**
     * 获取主键的值
     * @return
     * @throws SQLException
     */
    protected List<Object> getPkValuesByColumn() throws SQLException {
        // insert values including PK
        SQLInsertRecognizer recognizer = (SQLInsertRecognizer) sqlRecognizer;
        // 获取主键下标
        final int pkIndex = getPkIndex();
        // 代表一组主键
        List<Object> pkValues = null;
        // 获取设置完参数的会话对象
        if (statementProxy instanceof PreparedStatementProxy) {
            PreparedStatementProxy preparedStatementProxy = (PreparedStatementProxy) statementProxy;

            // 应该就是插入语句吧 这里需要了解 druid 内部实现
            List<List<Object>> insertRows = recognizer.getInsertRows();
            // 如果插入语句不为空
            if (insertRows != null && !insertRows.isEmpty()) {
                // 获取所有参数
                ArrayList<Object>[] parameters = preparedStatementProxy.getParameters();
                final int rowSize = insertRows.size();

                // 首先行数已经确定是1了
                if (rowSize == 1) {
                    // 获取第一行的主键
                    Object pkValue = insertRows.get(0).get(pkIndex);
                    // 如果是占位符
                    if (PLACEHOLDER.equals(pkValue)) {
                        pkValues = parameters[pkIndex];
                    } else {
                        int finalPkIndex = pkIndex;
                        pkValues = insertRows.stream().map(insertRow -> insertRow.get(finalPkIndex)).collect(Collectors.toList());
                    }
                    // 代表是多行
                } else {
                    int totalPlaceholderNum = -1;
                    pkValues = new ArrayList<>(rowSize);
                    for (int i = 0; i < rowSize; i++) {
                        List<Object> row = insertRows.get(i);
                        Object pkValue = row.get(pkIndex);
                        int currentRowPlaceholderNum = -1;
                        // 遍历所有行 一旦发现由占位符就增加 row 数量
                        for (Object r : row) {
                            if (PLACEHOLDER.equals(r)) {
                                totalPlaceholderNum += 1;
                                currentRowPlaceholderNum += 1;
                            }
                        }
                        if (PLACEHOLDER.equals(pkValue)) {
                            int idx = pkIndex;
                            if (i != 0) {
                                idx = totalPlaceholderNum - currentRowPlaceholderNum + pkIndex;
                            }
                            ArrayList<Object> parameter = parameters[idx];
                            for (Object obj : parameter) {
                                pkValues.add(obj);
                            }
                        } else {
                            pkValues.add(pkValue);
                        }
                    }
                }
            }
        } else {
            List<List<Object>> insertRows = recognizer.getInsertRows();
            pkValues = new ArrayList<>(insertRows.size());
            for (List<Object> row : insertRows) {
                pkValues.add(row.get(pkIndex));
            }
        }
        if (pkValues == null) {
            throw new ShouldNeverHappenException();
        }
        boolean b = this.checkPkValues(pkValues);
        if (!b) {
            throw new NotSupportYetException("not support sql [" + sqlRecognizer.getOriginalSQL() + "]");
        }
        if (pkValues.size() == 1 && pkValues.get(0) instanceof SqlSequenceExpr) {
            pkValues = getPkValuesBySequence(pkValues.get(0));
        }
        // pk auto generated while single insert primary key is expression
        else if (pkValues.size() == 1 && pkValues.get(0) instanceof SqlMethodExpr) {
            pkValues = getPkValuesByAuto();
        }
        // pk auto generated while column exists and value is null
        else if (pkValues.size() > 0 && pkValues.get(0) instanceof Null) {
            pkValues = getPkValuesByAuto();
        }
        return pkValues;
    }

    protected List<Object> getPkValuesBySequence(Object expr) throws SQLException {
        ResultSet genKeys = null;
        if (expr instanceof SqlSequenceExpr) {
            SqlSequenceExpr sequenceExpr = (SqlSequenceExpr) expr;
            final String sql = "SELECT " + sequenceExpr.getSequence() + ".currval FROM DUAL";
            LOGGER.warn("Fail to get auto-generated keys, use \'{}\' instead. Be cautious, statement could be polluted. Recommend you set the statement to return generated keys.", sql);
            genKeys = statementProxy.getConnection().createStatement().executeQuery(sql);
        } else {
            throw new NotSupportYetException(String.format("not support expr [%s]", expr.getClass().getName()));
        }
        List<Object> pkValues = new ArrayList<>();
        while (genKeys.next()) {
            Object v = genKeys.getObject(1);
            pkValues.add(v);
        }
        return pkValues;
    }

    /**
     * 当主键是自动增长时通过 jdbc层去获取 generateKey 的数值
     * @return
     * @throws SQLException
     */
    protected List<Object> getPkValuesByAuto() throws SQLException {
        // PK is just auto generated
        // 不存在 主键 抛出异常
        Map<String, ColumnMeta> pkMetaMap = getTableMeta().getPrimaryKeyMap();
        if (pkMetaMap.size() != 1) {
            throw new NotSupportYetException();
        }
        ColumnMeta pkMeta = pkMetaMap.values().iterator().next();
        // 主键非自动增加抛出异常
        if (!pkMeta.isAutoincrement()) {
            throw new ShouldNeverHappenException();
        }

        ResultSet genKeys = null;
        try {
            // 获取自动增加键 的数值 比如当前id 是多少
            genKeys = statementProxy.getTargetStatement().getGeneratedKeys();
        } catch (SQLException e) {
            // java.sql.SQLException: Generated keys not requested. You need to
            // specify Statement.RETURN_GENERATED_KEYS to
            // Statement.executeUpdate() or Connection.prepareStatement().
            if (ERR_SQL_STATE.equalsIgnoreCase(e.getSQLState())) {
                LOGGER.warn("Fail to get auto-generated keys, use \'SELECT LAST_INSERT_ID()\' instead. Be cautious, statement could be polluted. Recommend you set the statement to return generated keys.");
                genKeys = statementProxy.getTargetStatement().executeQuery("SELECT LAST_INSERT_ID()");
            } else {
                throw e;
            }
        }
        List<Object> pkValues = new ArrayList<>();
        while (genKeys.next()) {
            Object v = genKeys.getObject(1);
            pkValues.add(v);
        }
        return pkValues;
    }

    /**
     * get pk index
     * 获取主键的下标
     * @return -1 not found pk index
     */
    protected int getPkIndex() {
        SQLInsertRecognizer recognizer = (SQLInsertRecognizer) sqlRecognizer;
        // 从表的元数据信息中获取 pk字段的 name
        String pkName = getTableMeta().getPkName();
        // 获取所有插入col
        List<String> insertColumns = recognizer.getInsertColumns();
        // 当插入 col 不为空时
        if (insertColumns != null && !insertColumns.isEmpty()) {
            final int insertColumnsSize = insertColumns.size();
            int pkIndex = -1;
            for (int paramIdx = 0; paramIdx < insertColumnsSize; paramIdx++) {
                if (insertColumns.get(paramIdx).equalsIgnoreCase(pkName)) {
                    pkIndex = paramIdx;
                    break;
                }
            }
            // 获取主键下标
            return pkIndex;
        }
        int pkIndex = -1;
        Map<String, ColumnMeta> allColumns = getTableMeta().getAllColumns();
        for (Map.Entry<String, ColumnMeta> entry : allColumns.entrySet()) {
            pkIndex++;
            if (entry.getValue().getColumnName().equalsIgnoreCase(pkName)) {
                break;
            }
        }
        return pkIndex;
    }

    /**
     * check pk values
     * @param pkValues
     * @return true support false not support
     */
    private boolean checkPkValues(List<Object> pkValues) {
        boolean pkParameterHasNull = false;
        boolean pkParameterHasNotNull = false;
        boolean pkParameterHasExpr = false;
        if (pkValues.size() == 1) {
            return true;
        }
        for (Object pkValue : pkValues) {
            if (pkValue instanceof Null) {
                pkParameterHasNull = true;
                continue;
            }
            pkParameterHasNotNull = true;
            if (pkValue instanceof SqlMethodExpr) {
                pkParameterHasExpr = true;
            }
        }
        if (pkParameterHasExpr) {
            return false;
        }
        if (pkParameterHasNull && pkParameterHasNotNull) {
            return false;
        }
        return true;
    }

}
