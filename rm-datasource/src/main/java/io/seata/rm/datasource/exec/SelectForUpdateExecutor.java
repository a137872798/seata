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

import java.sql.Connection;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import io.seata.common.util.StringUtils;
import io.seata.core.context.RootContext;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.sql.SQLRecognizer;
import io.seata.rm.datasource.sql.SQLSelectRecognizer;
import io.seata.rm.datasource.sql.struct.TableRecords;

/**
 * The type Select for update executor.
 * 使用读已提交级别的 查询
 * @author sharajava
 *
 * @param <S> the type parameter
 */
public class SelectForUpdateExecutor<T, S extends Statement> extends BaseTransactionalExecutor<T, S> {

    /**
     * Instantiates a new Select for update executor.
     *
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param sqlRecognizer     the sql recognizer
     */
    public SelectForUpdateExecutor(StatementProxy<S> statementProxy, StatementCallback<T, S> statementCallback,
                                   SQLRecognizer sqlRecognizer) {
        super(statementProxy, statementCallback, sqlRecognizer);
    }

    /**
     * 基于读已提交的事务隔离级别进行查询
     * @param args the args
     * @return
     * @throws Throwable
     */
    @Override
    public T doExecute(Object... args) throws Throwable {
        // 获取连接对象
        Connection conn = statementProxy.getConnection();
        T rs = null;
        Savepoint sp = null;
        // 就是一个简单的 具备 sleep 次数 和sleep时间的对象
        LockRetryController lockRetryController = new LockRetryController();
        // 当前是否是自动提交
        boolean originalAutoCommit = conn.getAutoCommit();
        ArrayList<List<Object>> paramAppenderList = new ArrayList<>();
        // 构建需要使用的查询主键  在for update 时 需要查询这些主键是否正在被其他 DML 语句修改 如果正在修改是不允许查询的 必须等到事务提交完毕
        String selectPKSQL = buildSelectSQL(paramAppenderList);
        try {
            // 关闭自动提交
            if (originalAutoCommit) {
                conn.setAutoCommit(false);
            }
            // 为当前事务设置一个 保存点
            sp = conn.setSavepoint();

            while (true) {
                try {
                    // #870
                    // execute return Boolean
                    // executeQuery return ResultSet
                    // 走正常的查询
                    rs = statementCallback.execute(statementProxy.getTargetStatement(), args);

                    // Try to get global lock of those rows selected
                    // 获取全局锁需要锁定的字段
                    TableRecords selectPKRows = buildTableRecords(getTableMeta(), selectPKSQL, paramAppenderList);
                    // 生成锁语句
                    String lockKeys = buildLockKey(selectPKRows);
                    // 如果本身没有对任何主键加锁 直接退出循环 因为该while 内部是 try catch 了一个冲突异常那么 在发现异常后就是不断重试 直到获取锁成功
                    if (StringUtils.isNullOrEmpty(lockKeys)) {
                        break;
                    }

                    // 代表在全局事务中需要检查锁的状态
                    if (RootContext.inGlobalTransaction()) {
                        //do as usual  当访问TC 时发现锁已存在 就抛出锁冲突异常 这样会进入下次循环 直到 加了全局锁的某个事务释放锁
                        statementProxy.getConnectionProxy().checkLock(lockKeys);
                    // 如果使用了 @GlobalLock 注解 将需要加锁的字段设置到 connectionContext 中
                    } else if (RootContext.requireGlobalLock()) {
                        //check lock key before commit just like DML to avoid reentrant lock problem(no xid thus can
                        // not reentrant)
                        statementProxy.getConnectionProxy().appendLockKey(lockKeys);
                    } else {
                        throw new RuntimeException("Unknown situation!");
                    }
                    break;
                } catch (LockConflictException lce) {
                    // 出现异常时 先回滚到保存点 之后继续自旋 直到 能正常加锁
                    conn.rollback(sp);
                    lockRetryController.sleep(lce);
                }
            }
        } finally {
            if (sp != null) {
                conn.releaseSavepoint(sp);
            }
            if (originalAutoCommit) {
                conn.setAutoCommit(true);
            }
        }
        return rs;
    }

    /**
     * 构建查询的语句
     * @param paramAppenderList
     * @return
     */
    private String buildSelectSQL(ArrayList<List<Object>> paramAppenderList){
        // 该对象是 druid 解析sql 使用的类 内部应该存放了主键等关键信息
        SQLSelectRecognizer recognizer = (SQLSelectRecognizer)sqlRecognizer;
        StringBuilder selectSQLAppender = new StringBuilder("SELECT ");
        selectSQLAppender.append(getColumnNameInSQL(getTableMeta().getPkName()));
        selectSQLAppender.append(" FROM " + getFromTableInSQL());
        String whereCondition = buildWhereCondition(recognizer, paramAppenderList);
        if (StringUtils.isNotBlank(whereCondition)) {
            selectSQLAppender.append(" WHERE " + whereCondition);
        }
        selectSQLAppender.append(" FOR UPDATE");
        return selectSQLAppender.toString();
    }
}
