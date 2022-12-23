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

package com.bytedance.primus.apiserver.service;

import com.bytedance.primus.apiserver.proto.ApiServerConfProto.ApiServerConf;
import com.bytedance.primus.apiserver.utils.Constants;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiServer {

  private static final int INVALID_PORT = 0;

  private static final Logger LOG = LoggerFactory.getLogger(ApiServer.class);

  private final Server server;
  private final String hostname;
  private int port = INVALID_PORT;


  /**
   * Creates a Primus API server
   *
   * @param conf API server configuration.
   * @param port the assigned listening port, where 0 for assigned by system.
   */
  public ApiServer(ApiServerConf conf, int port) throws Exception {
    this.hostname = InetAddress.getLocalHost().getHostAddress();
    this.server = NettyServerBuilder
        .forAddress(new InetSocketAddress(hostname, port))
        .maxMessageSize(Constants.MAX_MESSAGE_SIZE)
        .addService(new ResourceService(conf))
        .build();
  }

  public void start() throws Exception {
    server.start();
    port = server.getPort();
    LOG.info("{} has been started on {}:{}",
        ApiServer.class.getName(),
        hostname,
        port
    );
  }

  public void stop() {
    server.shutdown();
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    if (port == INVALID_PORT) {
      throw new RuntimeException("API server hasn't be started yet.");
    }
    return port;
  }
}
