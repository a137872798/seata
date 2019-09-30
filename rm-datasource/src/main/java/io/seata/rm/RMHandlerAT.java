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
package io.seata.rm;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;

import io.seata.core.model.BranchType;
import io.seata.core.model.ResourceManager;
import io.seata.core.protocol.transaction.UndoLogDeleteRequest;
import io.seata.rm.datasource.DataSourceManager;
import io.seata.rm.datasource.DataSourceProxy;
import io.seata.rm.datasource.undo.UndoLogManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Rm handler at.
 * 代表RM类型的处理器
 * @author sharajava
 */
public class RMHandlerAT extends AbstractRMHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RMHandlerAT.class);

    private static final int LIMIT_ROWS = 3000;

    /**
     * 代表处理逻辑
     * @param request the request
     */
    @Override
    public void handle(UndoLogDeleteRequest request) {
        // 获取Manager对象
        DataSourceManager dataSourceManager = (DataSourceManager)getResourceManager();
        // Manager对象 以 resourceId 作为key value 则是对应的数据源对象 (因为创建该对象开销比较大 所以需要做缓存)
        // 这里数据源是 怎么产生的
        DataSourceProxy dataSourceProxy = dataSourceManager.get(request.getResourceId());
        // 如果缓存对象不存在 直接返回
        if (dataSourceProxy == null) {
            LOGGER.warn("Failed to get dataSourceProxy for delete undolog on " + request.getResourceId());
            return;
        }
        // 推算出日志的创建时间
        Date logCreatedSave = getLogCreated(request.getSaveDays());
        Connection conn = null;
        try {
            conn = dataSourceProxy.getPlainConnection();
            int deleteRows = 0;
            do {
                try {
                    // 删除日志
                    deleteRows = UndoLogManagerFactory.getUndoLogManager(dataSourceProxy.getDbType())
                            .deleteUndoLogByLogCreated(logCreatedSave, LIMIT_ROWS, conn);
                    // 如果没有开启自动提交 则手动提交  用户可能会处于某种原因设置 setAutoCommit 为false 比如 可能想要自己控制事务
                    if (deleteRows > 0 && !conn.getAutoCommit()) {
                        conn.commit();
                    }
                } catch (SQLException exx) {
                    if (deleteRows > 0 && !conn.getAutoCommit()) {
                        conn.rollback();
                    }
                    throw exx;
                }
            } while (deleteRows == LIMIT_ROWS);
        } catch (Exception e) {
            LOGGER.error("Failed to delete expired undo_log，error:{}", e.getMessage(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException closeEx) {
                    LOGGER.warn("Failed to close JDBC resource while deleting undo_log ", closeEx);
                }
            }
        }
    }

    /**
     * 计算日志生成时间
     * @param saveDays
     * @return
     */
    private Date getLogCreated(int saveDays) {
        if (saveDays <= 0) {
            saveDays = UndoLogDeleteRequest.DEFAULT_SAVE_DAYS;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, 0 - saveDays);
        return calendar.getTime();
    }

    /**
     * get AT resource managerDataSourceManager.java
     *
     * @return
     */
    @Override
    protected ResourceManager getResourceManager() {
        return DefaultResourceManager.get().getResourceManager(BranchType.AT);
    }

    @Override
    public BranchType getBranchType() {
        return BranchType.AT;
    }

}
