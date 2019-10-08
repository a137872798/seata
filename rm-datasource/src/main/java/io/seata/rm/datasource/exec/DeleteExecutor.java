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

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import com.alibaba.druid.util.JdbcConstants;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.sql.SQLDeleteRecognizer;
import io.seata.rm.datasource.sql.SQLRecognizer;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableRecords;
import io.seata.rm.datasource.undo.KeywordChecker;
import io.seata.rm.datasource.undo.KeywordCheckerFactory;
import org.apache.commons.lang.StringUtils;

/**
 * The type Delete executor.
 * 删除执行器 增强了 DML 执行器
 * @author sharajava
 *
 * @param <T> the type parameter
 * @param <S> the type parameter
 */
public class DeleteExecutor<T, S extends Statement> extends AbstractDMLBaseExecutor<T, S> {

    /**
     * Instantiates a new Delete executor.
     * 实例化一个 delete 执行器
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param sqlRecognizer     the sql recognizer
     */
    public DeleteExecutor(StatementProxy statementProxy, StatementCallback statementCallback,
                          SQLRecognizer sqlRecognizer) {
        super(statementProxy, statementCallback, sqlRecognizer);
    }

    /**
     * 生成快照对象
     * @return
     * @throws SQLException
     */
    @Override
    protected TableRecords beforeImage() throws SQLException {
        SQLDeleteRecognizer visitor = (SQLDeleteRecognizer) sqlRecognizer;
        // 通过connection 去查询表的元数据信息
        TableMeta tmeta = getTableMeta(visitor.getTableName());
        // 生成一个参数列表对象
        ArrayList<List<Object>> paramAppenderList = new ArrayList<>();
        // 构建一个 锁住所有col 的语句
        String selectSQL = buildBeforeImageSQL(visitor, tmeta, paramAppenderList);
        // 根据查询结果 设置 tableRecord 对象 并作为beforeImage
        return buildTableRecords(tmeta, selectSQL, paramAppenderList);
    }

    /**
     * 构建before 对象
     * @param visitor
     * @param tableMeta
     * @param paramAppenderList
     * @return
     */
    private String buildBeforeImageSQL(SQLDeleteRecognizer visitor, TableMeta tableMeta, ArrayList<List<Object>> paramAppenderList) {
        // 获取 检查对象
        KeywordChecker keywordChecker = KeywordCheckerFactory.getKeywordChecker(JdbcConstants.MYSQL);
        // 构建 where 条件语句
        String whereCondition = buildWhereCondition(visitor, paramAppenderList);
        // 生成一个 写锁语句
        StringBuilder suffix = new StringBuilder(" FROM " + keywordChecker.checkAndReplace(getFromTableInSQL()));
        if (StringUtils.isNotBlank(whereCondition)) {
            suffix.append(" WHERE " + whereCondition);
        }
        suffix.append(" FOR UPDATE");
        StringJoiner selectSQLAppender = new StringJoiner(", ", "SELECT ", suffix.toString());
        // 这里吧所有col 都锁住了???
        for (String column : tableMeta.getAllColumns().keySet()) {
            selectSQLAppender.add(getColumnNameInSQL(keywordChecker.checkAndReplace(column)));
        }
        return selectSQLAppender.toString();
    }

    /**
     * 返回一个空的image 对象
     * @param beforeImage the before image
     * @return
     * @throws SQLException
     */
    @Override
    protected TableRecords afterImage(TableRecords beforeImage) throws SQLException {
        return TableRecords.empty(getTableMeta());
    }
}
