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
package io.seata.rm.datasource.undo;

import com.alibaba.fastjson.JSON;
import io.seata.common.util.StringUtils;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.model.Result;
import io.seata.rm.datasource.DataCompareUtils;
import io.seata.rm.datasource.sql.struct.Field;
import io.seata.rm.datasource.sql.struct.KeyType;
import io.seata.rm.datasource.sql.struct.Row;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import java.sql.JDBCType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * The type Abstract undo executor.
 * 日志执行器
 *
 * @author sharajava
 * @author Geng Zhang
 */
public abstract class AbstractUndoExecutor {

    /**
     * Logger for AbstractUndoExecutor
     **/
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUndoExecutor.class);

    /**
     * template of check sql
     * <p>
     * TODO support multiple primary key
     */
    private static final String CHECK_SQL_TEMPLATE = "SELECT * FROM %s WHERE %s in (%s)";

    /**
     * Switch of undo data validation
     * 数据撤销的校验器
     */
    public static final boolean IS_UNDO_DATA_VALIDATION_ENABLE = ConfigurationFactory.getInstance()
            .getBoolean(ConfigurationKeys.TRANSACTION_UNDO_DATA_VALIDATION, true);

    /**
     * The Sql undo log.
     * 撤销日志
     */
    protected SQLUndoLog sqlUndoLog;

    /**
     * Build undo sql string.
     *
     * @return the string
     */
    protected abstract String buildUndoSQL();

    /**
     * Instantiates a new Abstract undo executor.
     *
     * @param sqlUndoLog the sql undo log
     */
    public AbstractUndoExecutor(SQLUndoLog sqlUndoLog) {
        this.sqlUndoLog = sqlUndoLog;
    }

    /**
     * Gets sql undo log.
     *
     * @return the sql undo log
     */
    public SQLUndoLog getSqlUndoLog() {
        return sqlUndoLog;
    }

    /**
     * Execute on.
     * 使用某个连接执行任务
     *
     * @param conn the conn
     * @throws SQLException the sql exception
     */
    public void executeOn(Connection conn) throws SQLException {

        // 未通过校验时 直接返回
        if (IS_UNDO_DATA_VALIDATION_ENABLE && !dataValidationAndGoOn(conn)) {
            return;
        }

        try {
            // 构建撤销的sql
            String undoSQL = buildUndoSQL();

            // 使用语句生成携带参数的statement
            PreparedStatement undoPST = conn.prepareStatement(undoSQL);

            // 获取待撤销的行  该方法由子类实现
            TableRecords undoRows = getUndoRows();

            for (Row undoRow : undoRows.getRows()) {
                ArrayList<Field> undoValues = new ArrayList<>();
                Field pkValue = null;
                for (Field field : undoRow.getFields()) {
                    if (field.getKeyType() == KeyType.PrimaryKey) {
                        pkValue = field;
                    } else {
                        undoValues.add(field);
                    }
                }

                // 设置参数
                undoPrepare(undoPST, undoValues, pkValue);

                undoPST.executeUpdate();
            }

        } catch (Exception ex) {
            if (ex instanceof SQLException) {
                throw (SQLException) ex;
            } else {
                throw new SQLException(ex);
            }

        }

    }

    /**
     * Undo prepare.
     * 为 撤销语句设置参数
     *
     * @param undoPST    the undo pst
     * @param undoValues the undo values
     * @param pkValue    the pk value
     * @throws SQLException the sql exception
     */
    protected void undoPrepare(PreparedStatement undoPST, ArrayList<Field> undoValues, Field pkValue)
            throws SQLException {
        int undoIndex = 0;
        for (Field undoValue : undoValues) {
            undoIndex++;

            // BLOB 和 CLOB 代表大数据字段
            // 如果是 BLOB 类型
            if (undoValue.getType() == JDBCType.BLOB.getVendorTypeNumber()) {
                SerialBlob serialBlob = (SerialBlob) undoValue.getValue();
                // 设置二进制流
                undoPST.setBlob(undoIndex, serialBlob.getBinaryStream());
                // 如果是 CLOB 类型
            } else if (undoValue.getType() == JDBCType.CLOB.getVendorTypeNumber()) {
                SerialClob serialClob = (SerialClob) undoValue.getValue();
                undoPST.setClob(undoIndex, serialClob.getCharacterStream());
            } else {
                // 普通情况直接设置参数
                undoPST.setObject(undoIndex, undoValue.getValue(), undoValue.getType());
            }

        }
        // PK is at last one.
        // INSERT INTO a (x, y, z, pk) VALUES (?, ?, ?, ?)
        // UPDATE a SET x=?, y=?, z=? WHERE pk = ?
        // DELETE FROM a WHERE pk = ?
        // 最后设置 pk
        undoIndex++;
        undoPST.setObject(undoIndex, pkValue.getValue(), pkValue.getType());
    }

    /**
     * Gets undo rows.
     *
     * @return the undo rows
     */
    protected abstract TableRecords getUndoRows();

    /**
     * Data validation.
     * 判断conn 是否满足校验条件
     *
     * @param conn the conn
     * @return return true if data validation is ok and need continue undo, and return false if no need continue undo.
     * @throws SQLException the sql exception such as has dirty data
     */
    protected boolean dataValidationAndGoOn(Connection conn) throws SQLException {

        // tableRecord 内部包含了 TableMeta 对象
        TableRecords beforeRecords = sqlUndoLog.getBeforeImage();
        TableRecords afterRecords = sqlUndoLog.getAfterImage();

        // Compare current data with before data
        // No need undo if the before data snapshot is equivalent to the after data snapshot.
        // 判断前后数据是否相同
        Result<Boolean> beforeEqualsAfterResult = DataCompareUtils.isRecordsEquals(beforeRecords, afterRecords);
        // 如果数据没有发生变化 不允许执行回滚操作
        if (beforeEqualsAfterResult.getResult()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Stop rollback because there is no data change " +
                        "between the before data snapshot and the after data snapshot.");
            }
            // no need continue undo.
            return false;
        }

        // Validate if data is dirty.
        // 查询当前记录
        TableRecords currentRecords = queryCurrentRecords(conn);
        // compare with current data and after image.
        // 判断数据是否发生变化
        Result<Boolean> afterEqualsCurrentResult = DataCompareUtils.isRecordsEquals(afterRecords, currentRecords);
        // 发生变化 才有必要回滚
        if (!afterEqualsCurrentResult.getResult()) {

            // If current data is not equivalent to the after data, then compare the current data with the before 
            // data, too. No need continue to undo if current data is equivalent to the before data snapshot
            Result<Boolean> beforeEqualsCurrentResult = DataCompareUtils.isRecordsEquals(beforeRecords, currentRecords);
            if (beforeEqualsCurrentResult.getResult()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Stop rollback because there is no data change " +
                            "between the before data snapshot and the current data snapshot.");
                }
                // no need continue undo.
                return false;
            } else {
                if (LOGGER.isInfoEnabled()) {
                    if (StringUtils.isNotBlank(afterEqualsCurrentResult.getErrMsg())) {
                        LOGGER.info(afterEqualsCurrentResult.getErrMsg(), afterEqualsCurrentResult.getErrMsgParams());
                    }
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("check dirty datas failed, old and new data are not equal," +
                            "tableName:[" + sqlUndoLog.getTableName() + "]," +
                            "oldRows:[" + JSON.toJSONString(afterRecords.getRows()) + "]," +
                            "newRows:[" + JSON.toJSONString(currentRecords.getRows()) + "].");
                }
                throw new SQLException("Has dirty records when undo.");
            }
        }
        return true;
    }

    /**
     * Query current records.
     * 查询当前记录
     *
     * @param conn the conn
     * @return the table records
     * @throws SQLException the sql exception
     */
    protected TableRecords queryCurrentRecords(Connection conn) throws SQLException {
        // 由子类实现
        TableRecords undoRecords = getUndoRows();
        // 获取table 的元数据信息
        TableMeta tableMeta = undoRecords.getTableMeta();
        // 获取主键值        ;
        String pkName = tableMeta.getPkName();
        // 获取主键类型
        int pkType = tableMeta.getColumnMeta(pkName).getDataType();

        // pares pk values
        // 取出所有row 中 pk的值
        Object[] pkValues = parsePkValues(getUndoRows());
        if (pkValues.length == 0) {
            // 返回空对象
            return TableRecords.empty(tableMeta);
        }
        StringBuffer replace = new StringBuffer();
        for (int i = 0; i < pkValues.length; i++) {
            replace.append("?,");
        }
        // build check sql
        String checkSQL = String.format(CHECK_SQL_TEMPLATE, sqlUndoLog.getTableName(), pkName,
                replace.substring(0, replace.length() - 1));

        PreparedStatement statement = null;
        ResultSet checkSet = null;
        TableRecords currentRecords;
        try {
            statement = conn.prepareStatement(checkSQL);
            for (int i = 1; i <= pkValues.length; i++) {
                statement.setObject(i, pkValues[i - 1], pkType);
            }
            // 执行sql 获取结果集
            checkSet = statement.executeQuery();
            // 生成record 对象
            currentRecords = TableRecords.buildRecords(tableMeta, checkSet);
        } finally {
            if (checkSet != null) {
                try {
                    checkSet.close();
                } catch (Exception e) {
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                }
            }
        }
        return currentRecords;
    }

    /**
     * Parse pk values object [ ].
     * 将所有 row 中 对应的 pk 取出来
     * @param records the records
     * @return the object [ ]
     */
    protected Object[] parsePkValues(TableRecords records) {
        String pkName = records.getTableMeta().getPkName();
        List<Row> undoRows = records.getRows();
        Object[] pkValues = new Object[undoRows.size()];
        for (int i = 0; i < undoRows.size(); i++) {
            List<Field> fields = undoRows.get(i).getFields();
            if (fields != null) {
                for (Field field : fields) {
                    if (StringUtils.equalsIgnoreCase(pkName, field.getName())) {
                        pkValues[i] = field.getValue();
                    }
                }
            }
        }
        return pkValues;
    }
}
