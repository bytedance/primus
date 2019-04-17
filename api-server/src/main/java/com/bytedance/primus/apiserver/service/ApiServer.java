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

  private static final Logger LOG = LoggerFactory.getLogger(ApiServer.class);

  private Server server;
  private InetSocketAddress address;
  private String hostName;
  private int port;

  public ApiServer() throws Exception {
    this(0, null);
  }

  public ApiServer(ApiServerConf conf) throws Exception {
    this(0, conf);
  }

  public ApiServer(int port) throws Exception {
    this(port, null);
  }

  public ApiServer(int port, ApiServerConf conf) throws Exception {
    address = new InetSocketAddress(port);
    server = NettyServerBuilder.forAddress(address)
        .maxMessageSize(Constants.MAX_MESSAGE_SIZE)
        .addService(new ResourceService(conf)).build();
    hostName = InetAddress.getLocalHost().getHostName();
  }

  public String getHostName() {
    return hostName;
  }

  public int getPort() {
    return port;
  }

  public void start() throws Exception {
    server.start();
    port = server.getPort();
    LOG.info(ApiServer.class.getName() + " started on " + hostName + ":" + port);
  }

  public void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }
}
