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
 * Default event bus implement with Guava EventBus.
 * 基于guava 的事件总线
 * @author zhengyangyong
 */
public class GuavaEventBus implements EventBus {
    private final com.google.common.eventbus.EventBus eventBus;

    /**
     * identifier 用于标记该事件唯一性
     * @param identifier
     */
    public GuavaEventBus(String identifier) {
        this.eventBus = new com.google.common.eventbus.EventBus(identifier);
    }

    /**
     * 为 事件总线设置一个订阅者
     * @param subscriber
     */
    @Override
    public void register(Object subscriber) {
        this.eventBus.register(subscriber);
    }

    @Override
    public void unregister(Object subscriber) {
        this.eventBus.unregister(subscriber);
    }

    /**
     * 往总线中传入任务
     * @param event
     */
    @Override
    public void post(Event event) {
        this.eventBus.post(event);
    }
}
