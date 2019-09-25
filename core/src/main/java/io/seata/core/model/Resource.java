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
package io.seata.core.model;

/**
 * Resource that can be managed by Resource Manager and involved into global transaction.
 * 代表便于被 RM（资源管理器） 管理的对象
 * @author sharajava
 */
public interface Resource {

    /**
     * Get the resource group id.
     * e.g. master and slave data-source should be with the same resource group id.
     * 该资源属于哪个资源组
     * @return resource group id.
     */
    String getResourceGroupId();

    /**
     * Get the resource id.
     * e.g. url of a data-source could be the id of the db data-source resource.
     * 定位资源的唯一标识
     * @return resource id.
     */
    String getResourceId();

    /**
     * get resource type, AT、TCC etc.
     * 该资源属于 AT 还是TCC
     * @return
     */
    BranchType getBranchType();

}
