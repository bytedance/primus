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
import com.bytedance.primus.am.apiserver.DataController;
import com.bytedance.primus.am.apiserver.JobController;
import com.bytedance.primus.apiserver.client.models.Data;
import com.bytedance.primus.apiserver.client.models.Job;
import com.bytedance.primus.webapp.StatusFilter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PUBLIC)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(toBuilder = true, access = AccessLevel.PRIVATE)
public class StatusBundle {

  @Getter
  private String primusConf;
  @Getter
  private boolean isHistory;
  @Getter
  private SummaryBundle summary;
  @Getter
  private Map<String, List<RoleBundle>> roleMap;
  @Getter
  private List<TaskBundle> tasks;
  @Getter
  private String job;
  @Getter
  private String data;

  public StatusBundle(
      AMContext context,
      String primusConfJsonString,
      SummaryBundle summaryBundle,
      Map<String, List<RoleBundle>> roleBundles,
      List<TaskBundle> taskBundles
  ) {
    this.isHistory = false;
    this.primusConf = primusConfJsonString;
    this.summary = summaryBundle;
    this.roleMap = roleBundles;
    this.tasks = taskBundles;

    this.job = Optional
        .ofNullable(context)
        .map(AMContext::getJobController)
        .map(JobController::getJob)
        .map(Job::toString)
        .orElse(null);

    this.data = Optional
        .ofNullable(context)
        .map(AMContext::getDataController)
        .map(DataController::getData)
        .map(Data::toString)
        .orElse(null);
  }

  /**
   * StatusBundle newHistoryBundle() is for post-processing StatusBundles. Being created by running
   * Primus applications, StatusBundles contains information for running Primus Applications; hence
   * adjustments are needed for History UI needed and other similar purposes.
   *
   * @param includeSummary whether to include SummaryBundle
   * @param includeRoles   whether to include RoleBundles
   */
  public StatusBundle newHistoryBundle(
      boolean includeSummary,
      boolean includeRoles
  ) {
    boolean isHistory = !summary.getFinalStatus().equals("IN_PROGRESS");
    return this.toBuilder()
        .isHistory(true)
        .summary(!includeSummary
            ? null
            : isHistory
                ? SummaryBundle.newHistoryBundle(summary)
                : summary)
        .roleMap(!includeRoles
            ? null
            : isHistory
                ? RoleBundle.newHistoryBundles(roleMap)
                : roleMap)
        .tasks(isHistory
            ? TaskBundle.newHistoryBundles(tasks)
            : tasks)
        .build();
  }

  public StatusBundle newHistoryBundle(StatusFilter filter) {
    return this.newHistoryBundle(
        filter.includeSummary,
        filter.includeRoles
    );
  }

  public StatusBundle newHistoryBundle() {
    return this.newHistoryBundle(
        true,//  includeSummary
        true//  includeRoles
    );
  }
}
