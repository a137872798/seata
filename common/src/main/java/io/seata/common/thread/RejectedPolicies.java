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
package io.seata.common.thread;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Policies for RejectedExecutionHandler
 * 自定义的线程池拒绝策略
 *
 * @author guoyao
 */
public final class RejectedPolicies {

    /**
     * when rejected happened ,add the new task and run the oldest task
     *
     * @return rejected execution handler
     */
    public static RejectedExecutionHandler runsOldestTaskPolicy() {
        return new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                if (executor.isShutdown()) {
                    return;
                }
                BlockingQueue<Runnable> workQueue = executor.getQueue();
                // 如果队列已满 直接拉取任务来处理 并将新任务添加到线程池中
                Runnable firstWork = workQueue.poll();
                boolean newTaskAdd = workQueue.offer(r);
                if (firstWork != null) {
                    firstWork.run();
                }
                if (!newTaskAdd) {
                    executor.execute(r);
                }
            }
        };
    }
}
