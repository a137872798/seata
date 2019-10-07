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
package io.seata.rm.datasource.sql.struct;

import io.seata.common.exception.ShouldNeverHappenException;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

/**
 * The type Table records.
 *
 * @author sharajava
 */
public class TableRecords {

    private transient TableMeta tableMeta;

    private String tableName;

    private List<Row> rows = new ArrayList<Row>();

    /**
     * Gets table name.
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Sets table name.
     *
     * @param tableName the table name
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Gets rows.
     *
     * @return the rows
     */
    public List<Row> getRows() {
        return rows;
    }

    /**
     * Sets rows.
     *
     * @param rows the rows
     */
    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    /**
     * Instantiates a new Table records.
     */
    public TableRecords() {

    }

    /**
     * Instantiates a new Table records.
     *
     * @param tableMeta the table meta
     */
    public TableRecords(TableMeta tableMeta) {
        setTableMeta(tableMeta);
    }

    /**
     * Sets table meta.
     *
     * @param tableMeta the table meta
     */
    public void setTableMeta(TableMeta tableMeta) {
        if (this.tableMeta != null) {
            throw new ShouldNeverHappenException();
        }
        this.tableMeta = tableMeta;
        this.tableName = tableMeta.getTableName();
    }

    /**
     * Size int.
     *
     * @return the int
     */
    public int size() {
        return rows.size();
    }

    /**
     * Add.
     *
     * @param row the row
     */
    public void add(Row row) {
        rows.add(row);
    }

    /**
     * Pk rows list.
     *
     * @return the list
     */
    public List<Field> pkRows() {
        final String pkName = getTableMeta().getPkName();
        List<Field> pkRows = new ArrayList<>();
        for (Row row : rows) {
            List<Field> fields = row.getFields();
            for (Field field : fields) {
                if (field.getName().equalsIgnoreCase(pkName)) {
                    pkRows.add(field);
                    break;
                }
            }
        }
        return pkRows;
    }

    /**
     * Gets table meta.
     *
     * @return the table meta
     */
    public TableMeta getTableMeta() {
        return tableMeta;
    }

    /**
     * Empty table records.
     *
     * @param tableMeta the table meta
     * @return the table records
     */
    public static TableRecords empty(TableMeta tableMeta) {
        return new EmptyTableRecords(tableMeta);
    }

    /**
     * Build records table records.
     * 通过 表的元数据信息 和 执行某条查询sql的结果集 构建tableRecord 对象
     * @param tmeta     the tmeta
     * @param resultSet the result set
     * @return the table records
     * @throws SQLException the sql exception
     */
    public static TableRecords buildRecords(TableMeta tmeta, ResultSet resultSet) throws SQLException {
        // 通过元数据信息 创建tableRecord
        TableRecords records = new TableRecords(tmeta);
        // 获取结果集的元数据信息
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        // 获取有多少行
        int columnCount = resultSetMetaData.getColumnCount();

        while (resultSet.next()) {
            List<Field> fields = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                // 获取 对应的 col 名
                String colName = resultSetMetaData.getColumnName(i);
                // 从tableMeta中获取对应的列元数据信息
                ColumnMeta col = tmeta.getColumnMeta(colName);
                // 构建一个  字段对象 设置列名
                Field field = new Field();
                field.setName(col.getColumnName());
                // 如果是主键 标记起来
                if (tmeta.getPkName().equalsIgnoreCase(field.getName())) {
                    field.setKeyType(KeyType.PrimaryKey);
                }
                // 设置数据类型
                field.setType(col.getDataType());
                // mysql will not run in this code
                // cause mysql does not use java.sql.Blob, java.sql.sql.Clob to process Blob and Clob column
                // 如果是大数据类型如果发现是 Blob类型  这里转换成 SerialBlob 好像是因为 mysql不能直接支持 Blob和 Clob
                if (col.getDataType() == JDBCType.BLOB.getVendorTypeNumber()) {
                    Blob blob = resultSet.getBlob(i);
                    if (blob != null) {
                        field.setValue(new SerialBlob(blob));
                    }

                } else if (col.getDataType() == JDBCType.CLOB.getVendorTypeNumber()) {
                    Clob clob = resultSet.getClob(i);
                    if (clob != null){
                        field.setValue(new SerialClob(clob));
                    }
                } else {
                    field.setValue(resultSet.getObject(i));
                }

                fields.add(field);
            }

            Row row = new Row();
            row.setFields(fields);

            records.add(row);
        }
        return records;
    }

    public static class EmptyTableRecords extends TableRecords {

        public EmptyTableRecords() {}

        public EmptyTableRecords(TableMeta tableMeta) {
            this.setTableMeta(tableMeta);
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public List<Field> pkRows() {
            return new ArrayList<>();
        }

        @Override
        public void add(Row row) {
            throw new UnsupportedOperationException("xxx");
        }

        @Override
        public TableMeta getTableMeta() {
            throw new UnsupportedOperationException("xxx");
        }
    }
}
