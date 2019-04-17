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
package com.bytedance.primus.webapp;

import static com.bytedance.primus.webapp.StatusServlet.tryCompressRes;

import com.bytedance.primus.webapp.bundles.StatusBundle;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.webapp.View;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusView extends View {

  private HistoryContext context;
  private static ObjectMapper mapper = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(StatusView.class);

  @Inject
  public StatusView(ViewContext ctx, HistoryContext hctx) {
    super(ctx);
    this.context = hctx;
  }

  @Override
  public void render() {
    String requestURI = request().getRequestURI();
    LOG.info("RequestURI: {}", requestURI);

    Optional<HistorySnapshot> snapshot;
    String[] components = requestURI.split("/");
    int appIdIndex = locateAppId(components);
    if (appIdIndex != -1 && components.length > appIdIndex) {
      String appId = components[appIdIndex];
      if (components.length > appIdIndex + 1 && StringUtils.isNumeric(components[appIdIndex + 1])) {
        String attemptId = components[appIdIndex + 1];
        snapshot = context.getSnapshot(appId, attemptId);
      } else {
        snapshot = context.getSnapshot(appId);
      }
    } else {
      String appId = components[components.length - 2];
      snapshot = context.getSnapshot(appId);
    }

    try {
      StatusBundle statusBundle = snapshot
          .map(HistorySnapshot::getStatus)
          .map(status -> status.newHistoryBundle(StatusFilter.parse(request())))
          .orElse(null);

      response().setContentType("application/json");
      response().setHeader("Content-Encoding", "gzip");
      tryCompressRes(
          mapper.writeValueAsBytes(statusBundle),
          outputStream());

    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }

  private int locateAppId(String[] pathes) {
    for (int i = 0; i < pathes.length; i++) {
      if ("app".equals(pathes[i])) {
        return i + 1;
      }
    }
    return -1;
  }
}
