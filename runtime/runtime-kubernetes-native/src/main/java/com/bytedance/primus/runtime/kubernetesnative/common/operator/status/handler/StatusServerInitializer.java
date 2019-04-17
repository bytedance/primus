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

package com.bytedance.primus.runtime.kubernetesnative.common.operator.status.handler;

import com.bytedance.primus.runtime.kubernetesnative.common.operator.status.model.APIServerEndPoint;
import com.bytedance.primus.runtime.kubernetesnative.common.operator.status.service.JobStatusManagerService;
import com.bytedance.primus.runtime.kubernetesnative.common.operator.status.service.impl.JobStatusManagerServiceImpl;
import io.grpc.netty.shaded.io.netty.channel.ChannelInitializer;
import io.grpc.netty.shaded.io.netty.channel.ChannelPipeline;
import io.grpc.netty.shaded.io.netty.channel.socket.SocketChannel;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpObjectAggregator;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpRequestDecoder;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpResponseEncoder;

public class StatusServerInitializer extends ChannelInitializer<SocketChannel> {

  private JobStatusManagerService jobStatusManagerService;

  public StatusServerInitializer(APIServerEndPoint apiServerEndPoint, String appName) {
    jobStatusManagerService = new JobStatusManagerServiceImpl(apiServerEndPoint, appName);
  }

  @Override
  protected void initChannel(SocketChannel socketChannel) throws Exception {
    ChannelPipeline pipeline = socketChannel.pipeline();
    pipeline.addLast(new HttpRequestDecoder());
    pipeline.addLast(new HttpObjectAggregator(65536));
    pipeline.addLast(new HttpResponseEncoder());
    pipeline.addLast(new StatusServerHandler(jobStatusManagerService));
  }
}

