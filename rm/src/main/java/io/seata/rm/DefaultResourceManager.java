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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.seata.common.exception.FrameworkException;
import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.common.util.CollectionUtils;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.BranchType;
import io.seata.core.model.Resource;
import io.seata.core.model.ResourceManager;

/**
 * default resource manager, adapt all resource managers
 * 默认的资源管理器 该对象相当于一个管理对象 适配所有的 资源管理器实现
 * @author zhangsen
 */
public class DefaultResourceManager implements ResourceManager {

    /**
     * all resource managers
     * 存放AT
     * 存放TCC     的资源管理器
     */
    protected static Map<BranchType, ResourceManager> resourceManagers
        = new ConcurrentHashMap<>();

    private DefaultResourceManager() {
        initResourceManagers();
    }

    /**
     * Get resource manager.
     *
     * @return the resource manager
     */
    public static DefaultResourceManager get() {
        return SingletonHolder.INSTANCE;
    }

    /**
     * only for mock
     *
     * @param branchType
     * @param rm
     */
    public static void mockResourceManager(BranchType branchType, ResourceManager rm) {
        resourceManagers.put(branchType, rm);
    }

    /**
     * 初始化资源管理器
     */
    protected void initResourceManagers() {
        //init all resource managers
        List<ResourceManager> allResourceManagers = EnhancedServiceLoader.loadAll(ResourceManager.class);
        if (CollectionUtils.isNotEmpty(allResourceManagers)) {
            for (ResourceManager rm : allResourceManagers) {
                resourceManagers.put(rm.getBranchType(), rm);
            }
        }
    }

    // 操作委托给实际实现类

    @Override
    public BranchStatus branchCommit(BranchType branchType, String xid, long branchId,
                                     String resourceId, String applicationData)
        throws TransactionException {
        return getResourceManager(branchType).branchCommit(branchType, xid, branchId, resourceId, applicationData);
    }

    /**
     * 接受由 TC 发起的全局回滚下发请求到某个 branch 中 回滚本地事务
     * @param branchType      the branch type
     * @param xid             Transaction id.
     * @param branchId        Branch id.
     * @param resourceId      Resource id.
     * @param applicationData Application data bind with this branch.
     * @return
     * @throws TransactionException
     */
    @Override
    public BranchStatus branchRollback(BranchType branchType, String xid, long branchId,
                                       String resourceId, String applicationData)
        throws TransactionException {
        return getResourceManager(branchType).branchRollback(branchType, xid, branchId, resourceId, applicationData);
    }

    /**
     * 将某个分支事务 注册到TC 上 同时会将该事务中需要的主键加锁
     * @param branchType the branch type
     * @param resourceId the resource id
     * @param clientId   the client id
     * @param xid        the xid
     * @param applicationData the context
     * @param lockKeys   the lock keys
     * @return
     * @throws TransactionException
     */
    @Override
    public Long branchRegister(BranchType branchType, String resourceId,
                               String clientId, String xid, String applicationData, String lockKeys)
        throws TransactionException {
        return getResourceManager(branchType).branchRegister(branchType, resourceId, clientId, xid, applicationData,
            lockKeys);
    }

    /**
     * 将结果通知到 TC
     * @param branchType      the branch type
     * @param xid             the xid
     * @param branchId        the branch id
     * @param status          the status
     * @param applicationData the application data
     * @throws TransactionException
     */
    @Override
    public void branchReport(BranchType branchType, String xid, long branchId, BranchStatus status,
                             String applicationData) throws TransactionException {
        getResourceManager(branchType).branchReport(branchType, xid, branchId, status, applicationData);
    }

    /**
     * 通过 RM 与 TC 交互 判断是否已经加锁
     * @param branchType the branch type
     * @param resourceId the resource id
     * @param xid        the xid
     * @param lockKeys   the lock keys
     * @return
     * @throws TransactionException
     */
    @Override
    public boolean lockQuery(BranchType branchType, String resourceId,
                             String xid, String lockKeys) throws TransactionException {
        return getResourceManager(branchType).lockQuery(branchType, resourceId, xid, lockKeys);
    }

    @Override
    public void registerResource(Resource resource) {
        getResourceManager(resource.getBranchType()).registerResource(resource);
    }

    @Override
    public void unregisterResource(Resource resource) {
        getResourceManager(resource.getBranchType()).unregisterResource(resource);
    }

    @Override
    public Map<String, Resource> getManagedResources() {
        Map<String, Resource> allResource = new HashMap<String, Resource>();
        for (ResourceManager rm : resourceManagers.values()) {
            Map<String, Resource> tempResources = rm.getManagedResources();
            if (tempResources != null) {
                allResource.putAll(tempResources);
            }
        }
        return allResource;
    }

    /**
     * get ResourceManager by Resource Type
     *
     * @param branchType
     * @return
     */
    public ResourceManager getResourceManager(BranchType branchType) {
        ResourceManager rm = resourceManagers.get(branchType);
        if (rm == null) {
            throw new FrameworkException("No ResourceManager for BranchType:" + branchType.name());
        }
        return rm;
    }

    @Override
    public BranchType getBranchType() {
        throw new FrameworkException("DefaultResourceManager isn't a real ResourceManager");
    }

    private static class SingletonHolder {
        private static DefaultResourceManager INSTANCE = new DefaultResourceManager();
    }

}
