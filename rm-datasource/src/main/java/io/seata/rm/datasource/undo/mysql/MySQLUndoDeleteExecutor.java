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
package io.seata.rm.datasource.undo.mysql;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.alibaba.druid.util.JdbcConstants;
import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.rm.datasource.sql.struct.Field;
import io.seata.rm.datasource.sql.struct.Row;
import io.seata.rm.datasource.sql.struct.TableRecords;
import io.seata.rm.datasource.undo.AbstractUndoExecutor;
import io.seata.rm.datasource.undo.KeywordChecker;
import io.seata.rm.datasource.undo.KeywordCheckerFactory;
import io.seata.rm.datasource.undo.SQLUndoLog;

/**
 * The type My sql undo delete executor.
 * 删除撤销语句
 * @author sharajava
 */
public class MySQLUndoDeleteExecutor extends AbstractUndoExecutor {

    /**
     * Instantiates a new My sql undo delete executor.
     *
     * @param sqlUndoLog the sql undo log
     */
    public MySQLUndoDeleteExecutor(SQLUndoLog sqlUndoLog) {
        super(sqlUndoLog);
    }

    /**
     * INSERT INTO a (x, y, z, pk) VALUES (?, ?, ?, ?)
     */
    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)";

    /**
     * Undo delete.
     *
     * Notice: PK is at last one.
     * @see AbstractUndoExecutor#undoPrepare
     *
     * @return sql
     */
    @Override
    protected String buildUndoSQL() {
        // 获取校验器对象
        KeywordChecker keywordChecker = KeywordCheckerFactory.getKeywordChecker(JdbcConstants.MYSQL);
        // 获取快照 （关于delete的回滚语句必须包含前照）
        TableRecords beforeImage = sqlUndoLog.getBeforeImage();
        List<Row> beforeImageRows = beforeImage.getRows();
        if (beforeImageRows == null || beforeImageRows.size() == 0) {
            throw new ShouldNeverHappenException("Invalid UNDO LOG");
        }
        Row row = beforeImageRows.get(0);
        // 获取所有非 主键字段
        List<Field> fields = new ArrayList<>(row.nonPrimaryKeys());
        Field pkField = row.primaryKeys().get(0);
        // PK is at last one.
        // 将 pk 放到末尾
        fields.add(pkField);

        // 识别 字段是否属于DB 关键字 如果是的话就用 `` 进行包裹
        String insertColumns = fields.stream()
            .map(field -> keywordChecker.checkAndReplace(field.getName()))
            .collect(Collectors.joining(", "));
        String insertValues = fields.stream().map(field -> "?")
            .collect(Collectors.joining(", "));

        // 获取删除的数据重新生成 插入语句 注意这里主键也还原了
        return String.format(INSERT_SQL_TEMPLATE, keywordChecker.checkAndReplace(sqlUndoLog.getTableName()),
                             insertColumns, insertValues);
    }

    @Override
    protected TableRecords getUndoRows() {
        return sqlUndoLog.getBeforeImage();
    }
}
