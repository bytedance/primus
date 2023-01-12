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

package com.bytedance.primus.runtime.yarncommunity.runtime.monitor;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import com.bytedance.primus.common.model.ApplicationConstants.Environment;
import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.runtime.monitor.MonitorInfoProvider;
import com.bytedance.primus.runtime.yarncommunity.am.YarnAMContext;
import com.google.common.base.Preconditions;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorInfoProviderImpl implements MonitorInfoProvider {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorInfoProviderImpl.class);

  private static final String URL_FORMAT_KEY_YARN_APPLICATION_ID = "\\{\\{YarnApplicationId\\}\\}";
  private static final String URL_FORMAT_KEY_YARN_USERNAME = "\\{\\{YarnUsername\\}\\}";
  private static final String URL_FORMAT_KEY_YARN_NODE_HOSTNAME = "\\{\\{YarnNodeHostname\\}\\}";
  private static final String URL_FORMAT_KEY_YARN_NODE_HTTP_PORT = "\\{\\{YarnNodeHttpPort\\}\\}";
  private static final String URL_FORMAT_KEY_YARN_NODE_ID = "\\{\\{YarnNodeId\\}\\}";
  private static final String URL_FORMAT_KEY_YARN_CONTAINER_ID = "\\{\\{YarnContainerId\\}\\}";

  private final YarnAMContext context;

  public MonitorInfoProviderImpl(AMContext context) {
    Preconditions.checkArgument(
        context instanceof YarnAMContext,
        "YarnAMContext is required for YarnCommunity::MonitorInfoProviderImpl");

    this.context = (YarnAMContext) context;
  }

  @Override
  public String getApplicationId() {
    return context.getAppAttemptId().getApplicationId().toString();
  }

  @Override
  public int getAttemptId() {
    return context.getAppAttemptId().getAttemptId();
  }

  @Override
  public String getExecutorNodeName(SchedulerExecutor schedulerExecutor) {
    return schedulerExecutor.getContainer().getNodeId().toString();
  }

  // History file paths ============================================================================
  // ===============================================================================================

  @Override
  public String getHistorySnapshotSubdirectoryName() {
    return context.getAppAttemptId().getApplicationId().toString();
  }

  @Override
  public String getHistorySnapshotFileName() {
    return String.format("%s_%d",
        context.getAppAttemptId().getApplicationId().toString(),
        context.getAppAttemptId().getAttemptId()
    );
  }

  // Primus Tracking UI ============================================================================
  // ===============================================================================================

  @Override
  public String getAmTrackingUrl() {
    return "NA"; // YARN doesn't allow customized AM URLs.
  }

  @Override
  public String getHistoryTrackingUrl() {
    return StringUtils.genFromTemplateAndDictionary(
        context.getPrimusConf()
            .getRuntimeConf()
            .getYarnCommunityConf()
            .getPrimusUiConf()
            .getHistoryTrackingUrlFormat(),
        getAmUrlFormatterDictionary()
    );
  }

  // Logs ==========================================================================================
  // ===============================================================================================

  @Override
  public String getAmLogUrl() {
    return StringUtils.genFromTemplateAndDictionary(
        context.getPrimusConf()
            .getRuntimeConf()
            .getYarnCommunityConf()
            .getPrimusUiConf()
            .getContainerLogUrlFormat(),
        getAmUrlFormatterDictionary()
    );
  }

  @Override
  public String getAmHistoryLogUrl() {
    return StringUtils.genFromTemplateAndDictionary(
        context.getPrimusConf()
            .getRuntimeConf()
            .getYarnCommunityConf()
            .getPrimusUiConf()
            .getHistoryContainerLogUrlFormat(),
        getAmUrlFormatterDictionary()
    );
  }

  @Override
  public String getExecutorLogUrl(SchedulerExecutor schedulerExecutor) {
    return StringUtils.genFromTemplateAndDictionary(
        context.getPrimusConf()
            .getRuntimeConf()
            .getYarnCommunityConf()
            .getPrimusUiConf()
            .getContainerLogUrlFormat(),
        getExecutorUrlFormatterDictionary(schedulerExecutor)
    );
  }

  @Override
  public String getExecutorHistoryLogUrl(SchedulerExecutor schedulerExecutor) {
    return StringUtils.genFromTemplateAndDictionary(
        context.getPrimusConf()
            .getRuntimeConf()
            .getYarnCommunityConf()
            .getPrimusUiConf()
            .getHistoryContainerLogUrlFormat(),
        getExecutorUrlFormatterDictionary(schedulerExecutor)
    );
  }

  private Map<String, String> getAmUrlFormatterDictionary() {
    return new HashMap<String, String>() {{
      put(URL_FORMAT_KEY_YARN_APPLICATION_ID,
          context.getAppAttemptId().getApplicationId().toString());
      put(URL_FORMAT_KEY_YARN_USERNAME,
          context.getUsername());
      put(URL_FORMAT_KEY_YARN_NODE_HOSTNAME,
          context.getEnvs().get(Environment.NM_HOST.name()));
      put(URL_FORMAT_KEY_YARN_NODE_HTTP_PORT,
          context.getEnvs().get(Environment.NM_HTTP_PORT.name()));
      put(URL_FORMAT_KEY_YARN_NODE_ID,
          String.format("%s:%s",
              context.getEnvs().get(Environment.NM_HOST.name()),
              context.getEnvs().get(Environment.NM_PORT.name())));
      put(URL_FORMAT_KEY_YARN_CONTAINER_ID,
          context.getContainerId().toString());
    }};
  }

  private Map<String, String> getExecutorUrlFormatterDictionary(
      SchedulerExecutor schedulerExecutor
  ) {
    return new HashMap<String, String>() {{
      put(URL_FORMAT_KEY_YARN_APPLICATION_ID,
          context.getAppAttemptId().getApplicationId().toString());
      put(URL_FORMAT_KEY_YARN_USERNAME,
          context.getUsername());
      put(URL_FORMAT_KEY_YARN_NODE_HOSTNAME,
          schedulerExecutor.getContainer().getNodeId().getHost());
      put(URL_FORMAT_KEY_YARN_NODE_HTTP_PORT,
          getExecutorHttpPortString(schedulerExecutor));
      put(URL_FORMAT_KEY_YARN_NODE_ID,
          schedulerExecutor.getContainer().getNodeId().toString());
      put(URL_FORMAT_KEY_YARN_CONTAINER_ID,
          schedulerExecutor.getContainer().getId().toString());
    }};
  }

  private String getExecutorHttpPortString(SchedulerExecutor schedulerExecutor) {
    String nodeHttpAddress = schedulerExecutor.getContainer().getNodeHttpAddress();
    String[] tokens = nodeHttpAddress.split(":");
    if (tokens.length != 2 || NumberUtils.isParsable(tokens[1])) {
      LOG.warn(
          "Failed to extract port from NodeHttpAddress({}), try using the http port of AM NM.",
          nodeHttpAddress);
      return context.getEnvs().get(Environment.NM_HTTP_PORT.name());
    }
    return tokens[1];
  }

  // Metrics =======================================================================================
  // ===============================================================================================

  @Override
  public Map<String, String> getAmDashboardUrls(String applicationId, Date startTime) {
    LOG.warn("AM dashboards are not supported yet.");
    return new HashMap<>();
  }

  @Override
  public Map<String, String> getExecutorDashboardUrls(
      String applicationId,
      SchedulerExecutor executor,
      Date startTime
  ) {
    LOG.warn("Executor dashboards are not supported yet.");
    return new HashMap<>();
  }
}
