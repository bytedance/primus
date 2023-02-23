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

import com.bytedance.primus.common.network.NetworkConfig;
import com.bytedance.primus.common.network.NetworkEndpointTypeEnum;
import com.bytedance.primus.common.service.AbstractService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMService extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(AMService.class);

  private final Server server;
  @Getter
  private final String hostName;
  @Getter
  private final int port;

  public AMService(AMContext context) throws IOException {
    super(AMService.class.getName());

    this.server = NettyServerBuilder
        .forAddress(new InetSocketAddress(0))
        .addService(new AMServiceServer(context))
        .build()
        .start();

    NetworkConfig networkConfig = context.getApplicationMeta().getNetworkConfig();
    this.hostName = NetworkEndpointTypeEnum.IPADDRESS == networkConfig.getNetworkEndpointType()
        ? InetAddress.getLocalHost().getHostAddress()
        : InetAddress.getLocalHost().getHostName();

    this.port = server.getPort();
    LOG.info("grpc server started on: {}:{}", hostName, port);
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
