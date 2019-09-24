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
package io.seata.core.event;

/**
 * The interface fot event bus.
 * 事件总线 类似 EventLoop
 * @author zhengyangyong
 */
public interface EventBus {
    void register(Object subscriber);

    void unregister(Object subscriber);

    /**
     * 应该是将事件传递到下一环
     * @param event
     */
    void post(Event event);
}
