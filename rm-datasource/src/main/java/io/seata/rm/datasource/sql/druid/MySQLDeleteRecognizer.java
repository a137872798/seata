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
package io.seata.rm.datasource.sql.druid;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import io.seata.rm.datasource.ParametersHolder;
import io.seata.rm.datasource.sql.SQLDeleteRecognizer;
import io.seata.rm.datasource.sql.SQLType;

/**
 * The type My sql delete recognizer.
 * 针对删除操作的识别对象
 * @author sharajava
 */
public class MySQLDeleteRecognizer extends BaseRecognizer implements SQLDeleteRecognizer {

    /**
     * 删除的会话对象  是 druid内部的类
     */
    private final MySqlDeleteStatement ast;

    /**
     * Instantiates a new My sql delete recognizer.
     *
     * @param originalSQL the original sql
     * @param ast         the ast
     */
    public MySQLDeleteRecognizer(String originalSQL, SQLStatement ast) {
        super(originalSQL);
        this.ast = (MySqlDeleteStatement) ast;
    }

    @Override
    public SQLType getSQLType() {
        return SQLType.DELETE;
    }

    @Override
    public String getTableAlias() {
        return ast.getTableSource().getAlias();
    }

    @Override
    public String getTableName() {
        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(sb) {

            @Override
            public boolean visit(SQLExprTableSource x) {
                printTableSourceExpr(x.getExpr());
                return false;
            }
        };
        // 将方法会将 表信息 设置到sb 中
        visitor.visit((SQLExprTableSource) ast.getTableSource());
        return sb.toString();
    }

    /**
     * 获取 where 条件
     * @param parametersHolder the parameters holder
     * @param paramAppenderList    the param appender list
     * @return
     */
    @Override
    public String getWhereCondition(final ParametersHolder parametersHolder, final ArrayList<List<Object>> paramAppenderList) {
        // 获取 where 表达式 如果没有直接返回 ""
        SQLExpr where = ast.getWhere();
        if (where == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        // 创建携带参数的 visitor 对象
        MySqlOutputVisitor visitor = super.createMySqlOutputVisitor(parametersHolder, paramAppenderList, sb);
        if (where instanceof SQLBinaryOpExpr) {
            visitor.visit((SQLBinaryOpExpr) where);
        } else if (where instanceof SQLInListExpr) {
            visitor.visit((SQLInListExpr) where);
        } else if (where instanceof SQLBetweenExpr) {
            visitor.visit((SQLBetweenExpr) where);
        } else {
            throw new IllegalArgumentException("unexpected WHERE expr: " + where.getClass().getSimpleName());
        }
        return sb.toString();
    }

    @Override
    public String getWhereCondition() {
        SQLExpr where = ast.getWhere();
        if (where == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(sb);
        visitor.visit((SQLBinaryOpExpr) where);
        return sb.toString();
    }

}
