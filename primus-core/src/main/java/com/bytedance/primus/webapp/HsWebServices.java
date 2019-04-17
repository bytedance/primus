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

import com.bytedance.primus.webapp.bundles.StatusBundle;
import com.bytedance.primus.webapp.dao.AppEntry;
import com.bytedance.primus.webapp.dao.AppList;
import com.bytedance.primus.webapp.dao.AppSnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.WebApp;

@Path("/ws/v1/history")
public class HsWebServices {

  private final HistoryContext ctx;
  private WebApp webapp;

  @Context
  private HttpServletResponse response;
  @Context
  private UriInfo uriInfo;

  private static ObjectMapper mapper = new ObjectMapper();

  @Inject
  public HsWebServices(final HistoryContext ctx, final Configuration conf, final WebApp webapp) {
    this.ctx = ctx;
    this.webapp = webapp;
  }

  private void init() {
    // clear content type
    response.setContentType(null);
  }

  @GET
  @Path("/list")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public AppList getJobs(@QueryParam("limit") String count) {
    long countParam = 100;
    init();

    if (count != null && !count.isEmpty()) {
      try {
        countParam = Long.parseLong(count);
      } catch (NumberFormatException e) {
        throw new BadRequestException(e.getMessage());
      }
      if (countParam <= 0) {
        throw new BadRequestException("limit value must be greater then 0");
      }
    }
    List<AppEntry> result = new ArrayList<>();
    for (HistorySnapshot snapshot : ctx.listRecentApps((int) countParam)) {
      result.add(new AppEntry(snapshot.getStatus().getSummary()));
    }
    return new AppList(result);
  }

  @GET
  @Path("/app/{appId}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public AppSnapshot getJob(
      @Context HttpServletRequest hsr,
      @PathParam("appId") String appId
  ) {
    init();
    try {
      return new AppSnapshot(
          mapper.writeValueAsString(ctx
              .getSnapshot(appId)
              .map(HistorySnapshot::getStatus)
              .map(StatusBundle::newHistoryBundle)
              .orElse(null)
          )
      );
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }

  @GET
  @Path("/app/{appId}/{attemptId}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public AppSnapshot getJob(
      @Context HttpServletRequest hsr,
      @PathParam("appId") String appId,
      @PathParam("attemptId") String attemptId
  ) {
    init();
    try {
      return new AppSnapshot(
          mapper.writeValueAsString(ctx
              .getSnapshot(appId, attemptId)
              .map(HistorySnapshot::getStatus)
              .map(StatusBundle::newHistoryBundle)
              .orElse(null)
          )
      );
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }
}
