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

package com.bytedance.primus.webapp.bundles;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.common.model.records.FinalApplicationStatus;
import com.bytedance.primus.runtime.monitor.MonitorInfoProvider;
import java.util.Date;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PUBLIC)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(toBuilder = true, access = AccessLevel.PRIVATE)
@Getter
public class SummaryBundle {

  private String applicationId;
  private String finalStatus;
  private String name;
  private String queue;
  private String user;
  private double progress;
  private Date startTime;
  private Date finishTime;
  private String amLogUrl;
  private String amHistoryLogUrl;
  private String jobHistoryUrl;
  private String amMonitorUrl;
  private int attemptId;
  private String jobMonitorUrl;
  private boolean starving = false;
  private String amWebshellUrl = "NA";
  private String amNodeId;
  private String version;
  private String exitCode;
  private String diagnostic;

  private SummaryBundle(AMContext context) {
    MonitorInfoProvider monitorInfoProvider = context.getMonitorInfoProvider();

    this.applicationId = monitorInfoProvider.getApplicationId();
    this.amLogUrl = monitorInfoProvider.getAmLogUrl();
    this.amHistoryLogUrl = monitorInfoProvider.getAmHistoryLogUrl();

    this.name = context.getApplicationMeta().getPrimusConf().getName();
    this.user = context.getApplicationMeta().getUsername();
    this.queue = context.getApplicationMeta().getPrimusConf().getQueue();
    this.finalStatus = Optional.of(context)
        .map(AMContext::getFinalStatus)
        .map(FinalApplicationStatus::toString)
        .orElse("IN_PROGRESS");
    this.version = context.getApplicationMeta().getVersion();
    this.diagnostic = context.getDiagnostic();
    this.exitCode = Optional.of(context)
        .map(AMContext::getExitCode)
        .map(String::valueOf)
        .orElse("-");
    this.finishTime = context.getFinishTime();
    this.progress = context.getProgressManager().getProgress();
    this.startTime = context.getStartTime();
    this.jobHistoryUrl = monitorInfoProvider.getHistoryTrackingUrl();
    this.attemptId = monitorInfoProvider.getAttemptId();

    this.amNodeId = context.getApplicationMeta().getNodeId();
    this.amMonitorUrl =
        // TODO: Support multi-dashboards on Primus UI
        String.join(", ", monitorInfoProvider.getAmDashboardUrls(
            this.applicationId,
            this.startTime
        ).values());

    this.jobMonitorUrl =
        // TODO: Support multi-dashboards on Primus UI
        String.join(", ", monitorInfoProvider.getAmDashboardUrls(
            this.applicationId,
            this.startTime
        ).values());
  }

  private SummaryBundle newHistoryBundle() {
    return this.toBuilder()
        .amLogUrl(this.amHistoryLogUrl)
        .build();
  }

  public static SummaryBundle newBundle(AMContext context) {
    return new SummaryBundle(context);
  }

  public static SummaryBundle newHistoryBundle(SummaryBundle original) {
    return original.newHistoryBundle();
  }
}
