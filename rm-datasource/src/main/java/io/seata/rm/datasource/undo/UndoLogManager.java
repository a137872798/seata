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

import io.seata.core.exception.TransactionException;
import io.seata.rm.datasource.ConnectionProxy;
import io.seata.rm.datasource.DataSourceProxy;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.Set;

/**
 * The type Undo log manager.
 * 撤销日志管理器 包含操作 撤销日志的相关api
 * @author sharajava
 * @author Geng Zhang
 */
public interface UndoLogManager {

    /**
     * Flush undo logs.
     * 执行当前维护的 撤销日志
     * @param cp the cp
     * @throws SQLException the sql exception
     */
    void flushUndoLogs(ConnectionProxy cp) throws SQLException;

    /**
     * Undo.
     * 生成撤销日志
     * @param dataSourceProxy the data source proxy
     * @param xid             the xid
     * @param branchId        the branch id
     * @throws TransactionException the transaction exception
     */
    void undo(DataSourceProxy dataSourceProxy, String xid, long branchId) throws TransactionException;

    /**
     * Delete undo log.
     * 删除撤销日志
     * @param xid      the xid
     * @param branchId the branch id
     * @param conn     the conn
     * @throws SQLException the sql exception
     */
    void deleteUndoLog(String xid, long branchId, Connection conn) throws SQLException;

    /**
     * batch Delete undo log.
     * 批量删除撤销日志
     * @param xids the xid set collections
     * @param branchIds the branch id set collections
     * @param conn the connection
     * @throws SQLException the sql exception
     */
    void batchDeleteUndoLog(Set<String> xids, Set<Long> branchIds, Connection conn) throws SQLException;

    /**
     * delete undolog by created
     * 通过指定创建时间  限制数量 来删除 日志文件
     * @param logCreated the created time
     * @param limitRows the limit rows
     * @param conn the connection
     * @return the update rows
     * @throws SQLException the sql exception
     */
    int deleteUndoLogByLogCreated(Date logCreated, int limitRows, Connection conn) throws SQLException;
}
