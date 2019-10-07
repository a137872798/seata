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

import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.dialect.oracle.visitor.OracleOutputVisitor;
import io.seata.rm.datasource.ParametersHolder;
import io.seata.rm.datasource.sql.SQLRecognizer;

import java.util.ArrayList;
import java.util.List;

/**
 * The type Base recognizer.
 * 解析器基类对象
 * @author sharajava
 */
public abstract class BaseRecognizer implements SQLRecognizer {

    /**
     * The type V marker.
     */
    public static class VMarker {
        @Override
        public String toString() {
            return "?";
        }

    }

    /**
     * The Original sql.
     * 原始的sql 语句
     */
    protected String originalSQL;

    /**
     * Instantiates a new Base recognizer.
     *
     * @param originalSQL the original sql
     */
    public BaseRecognizer(String originalSQL) {
        this.originalSQL = originalSQL;

    }

    @Override
    public String getOriginalSQL() {
        return originalSQL;
    }

    /**
     * 创建一个观察者对象
     * @param parametersHolder   携带参数的对象
     * @param paramAppenderList   参数列表
     * @param sb    应该是用于打印的
     * @return
     */
    public MySqlOutputVisitor createMySqlOutputVisitor(final ParametersHolder parametersHolder, final ArrayList<List<Object>> paramAppenderList, final StringBuilder sb) {
        // 创建抽象对象
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(sb) {

            // 将 druid 中某个类 作为参数 定制的 visit 方法
            @Override
            public boolean visit(SQLVariantRefExpr x) {
                // 如果表达式 为 ?
                if ("?".equals(x.getName())) {
                    // 从参数中找到对应的值
                    ArrayList<Object> oneParamValues = parametersHolder.getParameters()[x.getIndex()];
                    // 如果 appendList 还没有初始化 每发现一个 param 就往 appendList 中添加数组
                    if (paramAppenderList.size() == 0) {
                        oneParamValues.stream().forEach(t -> paramAppenderList.add(new ArrayList<>()));
                    }
                    // 这里将参数设置进去
                    for (int i = 0; i < oneParamValues.size(); i++) {
                        paramAppenderList.get(i).add(oneParamValues.get(i));
                    }

                }
                return super.visit(x);
            }
        };
        return visitor;
    }

    public OracleOutputVisitor createOracleOutputVisitor(final ParametersHolder parametersHolder, final ArrayList<List<Object>> paramAppenderList, final StringBuilder sb) {
        OracleOutputVisitor visitor = new OracleOutputVisitor(sb) {

            @Override
            public boolean visit(SQLVariantRefExpr x) {
                if ("?".equals(x.getName())) {
                    ArrayList<Object> oneParamValues = parametersHolder.getParameters()[x.getIndex()];
                    if (paramAppenderList.size() == 0) {
                        oneParamValues.stream().forEach(t -> paramAppenderList.add(new ArrayList<>()));
                    }
                    for (int i = 0; i < oneParamValues.size(); i++) {
                        paramAppenderList.get(i).add(oneParamValues.get(i));
                    }

                }
                return super.visit(x);
            }
        };
        return visitor;
    }
}
