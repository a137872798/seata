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
package io.seata.server;

import io.seata.common.XID;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.NetUtil;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.rpc.netty.RpcServer;
import io.seata.core.rpc.netty.ShutdownHook;
import io.seata.server.coordinator.DefaultCoordinator;
import io.seata.server.metrics.MetricsManager;
import io.seata.server.session.SessionHolder;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The type Server.
 *
 * @author jimin.jm @alibaba-inc.com
 */
public class Server {

    private static final int MIN_SERVER_POOL_SIZE = 100;
    private static final int MAX_SERVER_POOL_SIZE = 500;
    private static final int MAX_TASK_QUEUE_SIZE = 20000;
    private static final int KEEP_ALIVE_TIME = 500;
    /**
     * server 线程池 core 数量为100  该线程池是用于消费 client 发送的消息的
     */
    private static final ThreadPoolExecutor WORKING_THREADS = new ThreadPoolExecutor(MIN_SERVER_POOL_SIZE,
        MAX_SERVER_POOL_SIZE, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(MAX_TASK_QUEUE_SIZE),
        new NamedThreadFactory("ServerHandlerThread", MAX_SERVER_POOL_SIZE), new ThreadPoolExecutor.CallerRunsPolicy());

    /**
     * The entry point of application.
     * 服务器本身是通过 main 方法来启动的  而seats 在集成spring 后 如果配置的 scanner 对象就会自动启动client 并连接到 server
     * 至于如何连接到server 是通过在注册中心寻找对应的事务组 再进行注册进行关联的 (注意那个执行reconnection的定时器)
     * @param args the input arguments
     * @throws IOException the io exception
     */
    public static void main(String[] args) throws IOException {
        //initialize the metrics  TODO 统计先不看
        MetricsManager.get().init();

        //initialize the parameter parser
        // 解析启动server 使用参数
        ParameterParser parameterParser = new ParameterParser(args);

        // 获取server 数据持久化方式 (基于 文件 or db)
        System.setProperty(ConfigurationKeys.STORE_MODE, parameterParser.getStoreMode());

        RpcServer rpcServer = new RpcServer(WORKING_THREADS);
        //server port 监听命令行中的port  上面的初始化还没有进行bind 只是设置了server 启动需要的必备参数
        rpcServer.setListenPort(parameterParser.getPort());
        // serverNode 代表节点数量
        UUIDGenerator.init(parameterParser.getServerNode());
        //log store mode : file、db
        // 初始化会话保存对象 并且 加载之前处理到一半的会话数据
        SessionHolder.init(parameterParser.getStoreMode());

        // 使用服务器(这里将server 看作是一个serverMessageSender 对象) 对象去创建 TC
        DefaultCoordinator coordinator = new DefaultCoordinator(rpcServer);
        coordinator.init();
        rpcServer.setHandler(coordinator);
        // register ShutdownHook
        ShutdownHook.getInstance().addDisposable(coordinator);

        //127.0.0.1 and 0.0.0.0 are not valid here.
        if (NetUtil.isValidIp(parameterParser.getHost(), false)) {
            XID.setIpAddress(parameterParser.getHost());
        } else {
            XID.setIpAddress(NetUtil.getLocalIp());
        }
        XID.setPort(rpcServer.getListenPort());

        rpcServer.init();

        System.exit(0);
    }
}
