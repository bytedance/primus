/*
 * Copyright 2022 Bytedance Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.primus.runtime.kubernetesnative.common.operator;

import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.OPERATOR_STATE_API_SERVER_PORT;

import com.bytedance.primus.runtime.kubernetesnative.common.operator.status.handler.StatusServerInitializer;
import com.bytedance.primus.runtime.kubernetesnative.common.operator.status.model.APIServerEndPoint;
import io.grpc.netty.shaded.io.netty.bootstrap.ServerBootstrap;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.handler.logging.LogLevel;
import io.grpc.netty.shaded.io.netty.handler.logging.LoggingHandler;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator state server for k8s operator. port: 17080 host: driver-hostname
 */
public class OperatorAPIServer {

  private static Logger LOG = LoggerFactory.getLogger(OperatorAPIServer.class);
  private Channel ch;
  private ServerBootstrap b;
  private NioEventLoopGroup bossGroup;
  private NioEventLoopGroup workerGroup;
  private APIServerEndPoint apiServerEndPoint;
  private String appName;

  public OperatorAPIServer(APIServerEndPoint apiServerEndPoint, String appName) {
    this.apiServerEndPoint = apiServerEndPoint;
    this.appName = appName;
    init();
  }

  private void init() {
    int bossThreadsNum = 20;
    int workerThreadsNum = 100;

    bossGroup = new NioEventLoopGroup(bossThreadsNum);
    workerGroup = new NioEventLoopGroup(workerThreadsNum);
    LOG.info("bossThreads = {}, workerThreads = {}, webServerListenPort = {}",
        bossThreadsNum, workerThreadsNum, OPERATOR_STATE_API_SERVER_PORT);
    b = new ServerBootstrap();
    b.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new StatusServerInitializer(apiServerEndPoint, appName));
  }

  public void start() {
    try {
      ch = b.bind(OPERATOR_STATE_API_SERVER_PORT).sync().channel();
    } catch (InterruptedException ie) {
      LOG.error("Catch interrupted exception", ie);
    }
    String url = String.format("http://%s:%s/", "localhost", OPERATOR_STATE_API_SERVER_PORT);
    LOG.info("Start restful server, please access {}", url);

  }

  public void stop() {
    if (ch.isOpen()) {
      ch.close();
    }
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }

  public static void main(String[] args) {
    APIServerEndPoint apiServerEndPoint = new APIServerEndPoint("127.0.0.1", 1234);
    OperatorAPIServer server = new OperatorAPIServer(apiServerEndPoint, "primus-001");
    server.start();
    try {
      if (System.in.read() == 's') {
        server.stop();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}
