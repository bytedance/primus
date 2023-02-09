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
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PUBLIC)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(toBuilder = true, access = AccessLevel.PRIVATE)
@Getter
public class RoleBundle {

  private int id;
  private String node;
  private Date launchTime;
  private Date releaseTime;
  private int exitCode;
  private String state;
  private String diag;
  private String logUrl;
  private String historyLogUrl;
  private String monitorUrl;

  private RoleBundle(
      AMContext context,
      SummaryBundle summaryBundle,
      SchedulerExecutor schedulerExecutor
  ) {
    this.id = schedulerExecutor.getExecutorId().getIndex();
    this.launchTime = schedulerExecutor.getLaunchTime();
    this.state = schedulerExecutor.getExecutorState().name();
    this.exitCode = schedulerExecutor.getExecutorExitCode();
    this.diag = schedulerExecutor.getExecutorExitMsg();
    this.releaseTime = schedulerExecutor.getReleaseTime();

    this.logUrl = context
        .getMonitorInfoProvider()
        .getExecutorLogUrl(schedulerExecutor);

    this.historyLogUrl = context
        .getMonitorInfoProvider()
        .getExecutorHistoryLogUrl(schedulerExecutor);

    // TODO: Support multi-dashboards on Primus UI
    this.monitorUrl = String.join(", ",
        context
            .getMonitorInfoProvider()
            .getExecutorDashboardUrls(
                summaryBundle.getApplicationId(),
                schedulerExecutor,
                summaryBundle.getStartTime()
            ).values());

    this.node = context
        .getMonitorInfoProvider()
        .getExecutorNodeName(schedulerExecutor);
  }

  private RoleBundle newHistoryBundle() {
    return this.toBuilder()
        .logUrl(this.historyLogUrl)
        .build();
  }

  public static Map<String, List<RoleBundle>> newBundles(
      AMContext context,
      SummaryBundle summaryBundle
  ) {
    List<SchedulerExecutor> schedulerExecutors = new ArrayList<SchedulerExecutor>() {{
      addAll(context.getSchedulerExecutorManager().getRunningExecutorMap().values());
      addAll(context.getSchedulerExecutorManager().getCompletedExecutors());
    }};

    return schedulerExecutors.stream()
        // Create a new RoleBundle for each executor
        .map(executor -> Collections.singletonMap(
            executor.getExecutorId().getRoleName(),
            Collections.singletonList(new RoleBundle(context, summaryBundle, executor)))
        )
        // Group RoleBundles by their RoleName
        .reduce(new HashMap<>(), (acc, current) -> {
          // Merge by key
          current.forEach((k, v) -> {
            // Concat RoleBundles
            acc.merge(k, v, (vAcc, vCurrent) ->
                new LinkedList<RoleBundle>() {{
                  addAll(vAcc);
                  addAll(vCurrent);
                }});
          });
          return acc;
        });
  }

  public static Map<String, List<RoleBundle>> newHistoryBundles(
      Map<String, List<RoleBundle>> original
  ) {
    return original.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            entry -> entry.getValue().stream()
                .map(RoleBundle::newHistoryBundle)
                .collect(Collectors.toList())
        ));
  }
}

