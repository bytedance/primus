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
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorTrackerService extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorTrackerService.class);
  private static final int MAX_MESSAGE_SIZE = 1024 * 1024 * 128; // TODO: Centralize constants

  private final AMContext context;
  private Server server;

  public ExecutorTrackerService(AMContext context) {
    super(ExecutorTrackerService.class.getName());
    this.context = context;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    LOG.info("ExecutorTrackerService is initializing");
    server = NettyServerBuilder
        .forAddress(new InetSocketAddress(context.getExecutorTrackPort()))
        .maxMessageSize(MAX_MESSAGE_SIZE)
        .addService(new ExecutorTrackerGrpcService(context))
        .build();

    // NOTE: Since the port need to be acquired ASAP,
    // the GRPC server is spun up here instead of during serviceStart().
    LOG.info("ExecutorTrackerService is starting");
    server.start();
    context.setRpcAddress(
        new InetSocketAddress(
            InetAddress.getLocalHost().getHostName(),
            server.getPort()));

    LOG.info("ExecutorTrackerService is started, rpc address is:" + context.getRpcAddress());
  }

  @Override
  protected void serviceStart() {
    // NOTE: The server has already been started during serviceInit().
    super.start();
    LOG.info("ExecutorTrackerService has already been started");
  }

  @Override
  protected void serviceStop() {
    super.stop();
    server.shutdown();
    LOG.info("ExecutorTrackerService is stopped");
  }
}
