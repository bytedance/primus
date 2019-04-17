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

import com.bytedance.primus.runtime.kubernetesnative.common.operator.status.model.OperatorJobStatus;
import com.bytedance.primus.runtime.kubernetesnative.common.operator.status.service.JobStatusManagerService;
import com.bytedance.primus.runtime.kubernetesnative.common.operator.status.util.HTTPUtil;
import com.google.gson.Gson;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.ChannelInboundHandlerAdapter;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpRequest;
import io.grpc.netty.shaded.io.netty.handler.codec.http.QueryStringDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusServerHandler extends ChannelInboundHandlerAdapter {

  private static Logger LOG = LoggerFactory.getLogger(StatusServerHandler.class);
  private Pattern jobUriPattern = Pattern.compile("/operator/status");

  private Gson gson = new Gson();
  private JobStatusManagerService jobStatusManagerService;

  public StatusServerHandler(JobStatusManagerService jobStatusManagerService) {
    this.jobStatusManagerService = jobStatusManagerService;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    HttpRequest request = (HttpRequest) msg;
    String uri = request.uri();
    QueryStringDecoder decoder = new QueryStringDecoder(uri);
    String path = decoder.path();

    Matcher m = jobUriPattern.matcher(path);
    if (m.find()) {
      LOG.info("Receive request: {} ", path);
      String operatorJobStatus = getPrimusJobStatus();
      HTTPUtil.writeResponse(ctx, operatorJobStatus, "application/json");
    }
    HTTPUtil.writeResponse(ctx, "ERROR", "application/json");
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Catch exception, {}", cause);
    ctx.close();
  }

  private String getPrimusJobStatus() {
    OperatorJobStatus operatorJobStatus = new OperatorJobStatus();
    try {
      operatorJobStatus = jobStatusManagerService.fetchLatest();
      return gson.toJson(operatorJobStatus);
    } catch (Exception e) {
      operatorJobStatus.setMessage("fetch jobStatus failed! " + e.getMessage());
      LOG.error("Error when fetch JobStatus.", e);
    }
    return gson.toJson(operatorJobStatus);
  }
}
