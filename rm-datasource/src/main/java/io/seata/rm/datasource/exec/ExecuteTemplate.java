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

import io.seata.core.context.RootContext;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.sql.SQLRecognizer;
import io.seata.rm.datasource.sql.SQLVisitorFactory;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * The type Execute template.
 * 执行器模板  对应到RM 中全局事务开启后每个单独的事务执行
 * @author sharajava
 */
public class ExecuteTemplate {

    /**
     * Execute t.
     * 使用模板执行 jdbc查询 (在模板下会判断当前是否处在全局事务中etc)
     * @param <T>               the type parameter
     * @param <S>               the type parameter
     * @param statementProxy    the statement proxy     使用一个会话代理对象
     * @param statementCallback the statement callback   传入一个回调对象
     * @param args              the args    一组相关参数
     * @return the t
     * @throws SQLException the sql exception
     */
    public static <T, S extends Statement> T execute(StatementProxy<S> statementProxy,
                                                     StatementCallback<T, S> statementCallback,
                                                     Object... args) throws SQLException {
        return execute(null, statementProxy, statementCallback, args);
    }

    /**
     * Execute t.
     * 这里是在 全局事务中的分事务下 开始执行自己的会话
     * @param <T>               the type parameter
     * @param <S>               the type parameter
     * @param sqlRecognizer     the sql recognizer    sql 解析器 看来还支持sql本身是动态的 sql语句配合该解析器 已经传入的一组参数 执行sql
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param args              the args
     * @return the t
     * @throws SQLException the sql exception
     */
    public static <T, S extends Statement> T execute(SQLRecognizer sqlRecognizer,
                                                     StatementProxy<S> statementProxy,
                                                     StatementCallback<T, S> statementCallback,
                                                     Object... args) throws SQLException {

        // 非开启分布式事务 且 不需要获取全局锁
        if (!RootContext.inGlobalTransaction() && !RootContext.requireGlobalLock()) {
            // Just work as original statement
            // 走普通的逻辑
            return statementCallback.execute(statementProxy.getTargetStatement(), args);
        }

        // 代表要处理一个分布式事务

        // 如果解析器不存在的情况下  从 解析器工厂中生成对应的解析器对象
        if (sqlRecognizer == null) {
            sqlRecognizer = SQLVisitorFactory.get(
                    statementProxy.getTargetSQL(),
                    statementProxy.getConnectionProxy().getDbType());
        }
        Executor<T> executor = null;
        if (sqlRecognizer == null) {
            executor = new PlainExecutor<T, S>(statementProxy, statementCallback);
        } else {
            // 生成对应的 执行器对象  这些执行器 会在执行对应的操作时生成前后快照 并包装成undo日志 之后如果需要回滚本地事务 就可以根据undo日志还原 且对业务隐藏
            switch (sqlRecognizer.getSQLType()) {
                case INSERT:
                    executor = new InsertExecutor<T, S>(statementProxy, statementCallback, sqlRecognizer);
                    break;
                case UPDATE:
                    executor = new UpdateExecutor<T, S>(statementProxy, statementCallback, sqlRecognizer);
                    break;
                case DELETE:
                    executor = new DeleteExecutor<T, S>(statementProxy, statementCallback, sqlRecognizer);
                    break;
                case SELECT_FOR_UPDATE:
                    executor = new SelectForUpdateExecutor<T, S>(statementProxy, statementCallback, sqlRecognizer);
                    break;
                default:
                    executor = new PlainExecutor<T, S>(statementProxy, statementCallback);
                    break;
            }
        }
        T rs = null;
        try {
            // 执行专门的 executor 对象 该对象应该会生成 undo日志 便于回滚
            rs = executor.execute(args);
        } catch (Throwable ex) {
            if (!(ex instanceof SQLException)) {
                // Turn other exception into SQLException
                ex = new SQLException(ex);
            }
            throw (SQLException)ex;
        }
        return rs;
    }
}
