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

package com.bytedance.primus.runtime.monitor;


import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import java.util.Date;
import java.util.Map;

/**
 * MonitorInfoProvider is the interface for primus runtimes to configure monitor tooling surrounding
 * Primus applications which include but not limited to log URLs and dashboard URLs.
 */
public interface MonitorInfoProvider {

  String getApplicationId();

  int getAttemptId();

  String getExecutorNodeName(SchedulerExecutor schedulerExecutor);

  // History file paths ============================================================================
  // ===============================================================================================

  /**
   * Primus Applications periodically snapshot their latest status to the configured storage. Since
   * runtime container models differ from one runtime to the other, each runtime should instruct
   * where to persist the snapshots as shown below. If there are multiple snapshot files under
   * getSnapshotSubdirectoryName(), history-server defaults to the snapshot file of
   * getHistorySnapshotFileName() with the largest numerical suffix (_*), or falls back to file
   * modification time when failing to parse the suffixes.
   * <p>
   * PrimusConf::HistoryHdfsBase/getHistorySnapshotSubdirectoryName()/getHistorySnapshotFileName()
   */

  String getHistorySnapshotSubdirectoryName();

  // Alphabet increasing
  String getHistorySnapshotFileName();

  // Tracking UIs ==================================================================================
  // ===============================================================================================

  String getAmTrackingUrl();

  String getHistoryTrackingUrl();

  // Logs ==========================================================================================
  // ===============================================================================================

  String getAmLogUrl();

  String getAmHistoryLogUrl();

  String getExecutorLogUrl(SchedulerExecutor schedulerExecutor);

  String getExecutorHistoryLogUrl(SchedulerExecutor schedulerExecutor);

  // Metrics =======================================================================================
  // ===============================================================================================

  /**
   * Returns dashboard urls of Primus Application Master(AM).
   *
   * @param applicationId the applicationId of the Primus application
   * @param startTime     the startTime of the dashboard
   * @return a map of dashboard urls in the format of {name, url}
   */
  Map<String, String> getAmDashboardUrls(String applicationId, Date startTime);

  /**
   * Returns dashboard urls of a specific executor of a Primus Application.
   *
   * @param applicationId     the applicationId of the Primus application
   * @param schedulerExecutor the specified schedulerExecutor
   * @param startTime         the startTime of the dashboard
   * @return a map of dashboard urls in the format of {name, url}
   */
  Map<String, String> getExecutorDashboardUrls(
      String applicationId,
      SchedulerExecutor schedulerExecutor,
      Date startTime
  );
}
