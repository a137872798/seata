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
package io.seata.core.store.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import io.seata.common.exception.StoreException;
import io.seata.common.executor.Initialize;
import io.seata.common.loader.LoadLevel;
import io.seata.common.util.CollectionUtils;
import io.seata.common.util.StringUtils;
import io.seata.config.Configuration;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.constants.ServerTableColumnsName;
import io.seata.core.store.LockDO;
import io.seata.core.store.LockStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Data base lock store.
 * 基于 database 的 日志存储
 * 实际上基于 database 的加锁 释放锁 就是查询database 中是否已经包含了某些记录 删除记录就对应释放锁
 * @author zhangsen
 * @date 2019 /4/25
 */
@LoadLevel(name = "db")
public class LockStoreDataBaseDAO implements LockStore, Initialize {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockStoreDataBaseDAO.class);

    /**
     * The constant CONFIG.
     */
    protected static final Configuration CONFIG = ConfigurationFactory.getInstance();

    /**
     * The Log store data source.
     * 数据源对象 就是通过操作该对象 访问数据库
     */
    protected DataSource logStoreDataSource = null;

    /**
     * The Lock table.
     */
    protected String lockTable;

    /**
     * The Db type.
     */
    protected String dbType;

    /**
     * Instantiates a new Data base lock store dao.
     * 使用dataSource 来初始化
     * @param logStoreDataSource the log store data source
     */
    public LockStoreDataBaseDAO(DataSource logStoreDataSource) {
        this.logStoreDataSource = logStoreDataSource;
    }

    /**
     * 进行初始化
     */
    @Override
    public void init() {
        lockTable = CONFIG.getConfig(ConfigurationKeys.LOCK_DB_TABLE, ConfigurationKeys.LOCK_DB_DEFAULT_TABLE);
        dbType = CONFIG.getConfig(ConfigurationKeys.STORE_DB_TYPE);
        if (StringUtils.isBlank(dbType)) {
            throw new StoreException("there must be db type.");
        }
        if (logStoreDataSource == null) {
            throw new StoreException("there must be logStoreDataSource.");
        }
    }

    @Override
    public boolean acquireLock(LockDO lockDO) {
        return acquireLock(Collections.singletonList(lockDO));
    }

    /**
     * 尝试获取锁
     * @param lockDOs the lock d os
     * @return
     */
    @Override
    public boolean acquireLock(List<LockDO> lockDOs) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<LockDO> unrepeatedLockDOs = null;
        Set<String> dbExistedRowKeys = new HashSet<>();
        boolean originalAutoCommit = true;
        try {
            // 使用datasource 获取联俄籍
            conn = logStoreDataSource.getConnection();
            // 设置非自动提交
            if (originalAutoCommit = conn.getAutoCommit()) {
                conn.setAutoCommit(false);
            }
            //check lock
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < lockDOs.size(); i++) {
                sb.append("?");
                if (i != (lockDOs.size() - 1)) {
                    sb.append(", ");
                }
            }
            boolean canLock = true;
            //query
            // 创建一个检查是否可锁的 sql 语句
            String checkLockSQL = LockStoreSqls.getCheckLockableSql(lockTable, sb.toString(), dbType);
            // 生成会话对象
            ps = conn.prepareStatement(checkLockSQL);
            for (int i = 0; i < lockDOs.size(); i++) {
                // 设置参数信息
                ps.setString(i + 1, lockDOs.get(i).getRowKey());
            }
            // 执行查询
            rs = ps.executeQuery();
            // 看来这一组对象共用一个 xid 代表同属于一个事务吧
            String currentXID = lockDOs.get(0).getXid();
            while (rs.next()) {
                // 针对每行结果 获取 xid 信息
                String dbXID = rs.getString(ServerTableColumnsName.LOCK_TABLE_XID);
                if (!StringUtils.equals(dbXID, currentXID)) {
                    if (LOGGER.isInfoEnabled()) {
                        String dbPk = rs.getString(ServerTableColumnsName.LOCK_TABLE_PK);
                        String dbTableName = rs.getString(ServerTableColumnsName.LOCK_TABLE_TABLE_NAME);
                        Long dbBranchId = rs.getLong(ServerTableColumnsName.LOCK_TABLE_BRANCH_ID);
                        LOGGER.info("Global lock on [{}:{}] is holding by xid {} branchId {}", dbTableName, dbPk, dbXID,
                            dbBranchId);
                    }
                    canLock &= false;
                    break;
                }
                dbExistedRowKeys.add(rs.getString(ServerTableColumnsName.LOCK_TABLE_ROW_KEY));
            }

            if (!canLock) {
                // 回滚
                conn.rollback();
                return false;
            }
            if (CollectionUtils.isNotEmpty(dbExistedRowKeys)) {
                unrepeatedLockDOs = lockDOs.stream().filter(lockDO -> !dbExistedRowKeys.contains(lockDO.getRowKey()))
                    .collect(Collectors.toList());
            } else {
                unrepeatedLockDOs = lockDOs;
            }
            if (CollectionUtils.isEmpty(unrepeatedLockDOs)) {
                conn.rollback();
                return true;
            }

            //lock
            for (LockDO lockDO : unrepeatedLockDOs) {
                if (!doAcquireLock(conn, lockDO)) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Global lock acquire failed, xid {} branchId {} pk {}", lockDO.getXid(),
                            lockDO.getBranchId(), lockDO.getPk());
                    }
                    conn.rollback();
                    return false;
                }
            }
            // 代表成功获取了锁
            conn.commit();
            return true;
        } catch (SQLException e) {
            throw new StoreException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
            if (conn != null) {
                try {
                    if (originalAutoCommit) {
                        // 记得还原成自动提交  当autoCommit 设置成false 相当于会先开启事务
                        conn.setAutoCommit(true);
                    }
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    @Override
    public boolean unLock(LockDO lockDO) {
        return unLock(Collections.singletonList(lockDO));
    }

    @Override
    public boolean unLock(List<LockDO> lockDOs) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = logStoreDataSource.getConnection();
            conn.setAutoCommit(true);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < lockDOs.size(); i++) {
                sb.append("?");
                if (i != (lockDOs.size() - 1)) {
                    sb.append(", ");
                }
            }
            //batch release lock
            String batchDeleteSQL = LockStoreSqls.getBatchDeleteLockSql(lockTable, sb.toString(), dbType);
            ps = conn.prepareStatement(batchDeleteSQL);
            ps.setString(1, lockDOs.get(0).getXid());
            for (int i = 0; i < lockDOs.size(); i++) {
                ps.setString(i + 2, lockDOs.get(i).getRowKey());
            }
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new StoreException(e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    @Override
    public boolean isLockable(List<LockDO> lockDOs) {
        Connection conn = null;
        try {
            conn = logStoreDataSource.getConnection();
            conn.setAutoCommit(true);
            // 注意该语句会直接提交
            if (!checkLockable(conn, lockDOs)) {
                return false;
            }
            return true;
        } catch (SQLException e) {
            throw new StoreException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    /**
     * Do acquire lock boolean.
     * 尝试获取锁 这里就是执行一个插入语句
     * @param conn   the conn
     * @param lockDO the lock do
     * @return the boolean
     */
    protected boolean doAcquireLock(Connection conn, LockDO lockDO) {
        PreparedStatement ps = null;
        try {
            //insert
            String insertLockSQL = LockStoreSqls.getInsertLockSQL(lockTable, dbType);
            ps = conn.prepareStatement(insertLockSQL);
            ps.setString(1, lockDO.getXid());
            ps.setLong(2, lockDO.getTransactionId());
            ps.setLong(3, lockDO.getBranchId());
            ps.setString(4, lockDO.getResourceId());
            ps.setString(5, lockDO.getTableName());
            ps.setString(6, lockDO.getPk());
            ps.setString(7, lockDO.getRowKey());
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new StoreException(e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    /**
     * Check lock boolean.
     * 查询是否存在给定字段
     * @param conn    the conn
     * @param lockDOs the lock do
     * @return the boolean
     */
    protected boolean checkLockable(Connection conn, List<LockDO> lockDOs) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < lockDOs.size(); i++) {
                sb.append("?");
                if (i != (lockDOs.size() - 1)) {
                    sb.append(", ");
                }
            }

            //query
            String checkLockSQL = LockStoreSqls.getCheckLockableSql(lockTable, sb.toString(), dbType);
            ps = conn.prepareStatement(checkLockSQL);
            for (int i = 0; i < lockDOs.size(); i++) {
                ps.setString(i + 1, lockDOs.get(i).getRowKey());
            }
            rs = ps.executeQuery();
            // 这里应该是 隐藏了 重入锁 和 无锁的情况  只有有数据且 xid 不同的情况 才代表不能加锁
            while (rs.next()) {
                String xid = rs.getString("xid");
                if (!StringUtils.equals(xid, lockDOs.get(0).getXid())) {
                    return false;
                }
            }
            return true;
        } catch (SQLException e) {
            throw new StoreException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    /**
     * Sets lock table.
     *
     * @param lockTable the lock table
     */
    public void setLockTable(String lockTable) {
        this.lockTable = lockTable;
    }

    /**
     * Sets db type.
     *
     * @param dbType the db type
     */
    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    /**
     * Sets log store data source.
     *
     * @param logStoreDataSource the log store data source
     */
    public void setLogStoreDataSource(DataSource logStoreDataSource) {
        this.logStoreDataSource = logStoreDataSource;
    }
}
