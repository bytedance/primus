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

package com.bytedance.primus.am;

import com.bytedance.primus.common.service.AbstractService;
import com.bytedance.primus.proto.PrimusCommon.RunningMode;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMService extends AbstractService {

  private AMContext context;
  private Server server;
  private InetSocketAddress address;
  private String hostName;
  private int port;
  private static final Logger LOG = LoggerFactory.getLogger(AMService.class);

  public AMService(AMContext context) {
    super(AMService.class.getName());
    this.context = context;
  }

  public String getHostName() {
    return hostName;
  }

  public int getPort() {
    return port;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    address = new InetSocketAddress(0);
    server = NettyServerBuilder.forAddress(address)
        .addService(new AMSerivceServer(context))
        .build();
    if (context.getPrimusConf().getRunningMode() == RunningMode.KUBERNETES) {
      this.hostName = InetAddress.getLocalHost().getHostAddress();
    } else {
      this.hostName = InetAddress.getLocalHost().getHostName();
    }

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    server.start();
    this.port = server.getPort();
    String serviceAddress = hostName + ":" + port;
    LOG.info("grpc server started on: " + serviceAddress);
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (server != null) {
      String serviceAddress = hostName + ":" + port;
      LOG.info("grpc server stop on: " + serviceAddress);
      server.shutdown();
    }
  }
}
