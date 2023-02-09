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
 * See the License for the applicationMetaific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.primus.runtime.kubernetesnative.runtime.monitor;

import com.bytedance.primus.am.PrimusApplicationMeta;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.runtime.kubernetesnative.common.utils.ResourceNameBuilder;
import com.bytedance.primus.runtime.kubernetesnative.runtime.dictionary.Dictionary;
import com.bytedance.primus.runtime.monitor.MonitorInfoProvider;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorInfoProviderImpl implements MonitorInfoProvider {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorInfoProviderImpl.class);

  private final PrimusApplicationMeta applicationMeta;
  private final String kubernetesNamespace;
  private final String kubernetesDriverPodName;

  public MonitorInfoProviderImpl(PrimusApplicationMeta applicationMeta) {
    this.applicationMeta = applicationMeta;
    this.kubernetesNamespace = applicationMeta
        .getPrimusConf()
        .getRuntimeConf()
        .getKubernetesNativeConf()
        .getNamespace();
    this.kubernetesDriverPodName = ResourceNameBuilder
        .buildDriverPodName(applicationMeta.getApplicationId());
  }

  @Override
  public String getApplicationId() {
    return applicationMeta.getApplicationId();
  }

  @Override
  public int getAttemptId() {
    return applicationMeta.getAttemptId();
  }

  @Override
  public String getExecutorNodeName(SchedulerExecutor schedulerExecutor) {
    return schedulerExecutor.getContainer().getNodeId().getHost();
  }

  // History file paths ============================================================================
  // ===============================================================================================

  @Override
  public String getHistorySnapshotSubdirectoryName() {
    return applicationMeta.getApplicationId();
  }

  @Override
  public String getHistorySnapshotFileName() {
    return String.format("%s_%d",
        applicationMeta.getApplicationId(),
        applicationMeta.getAttemptId()
    );
  }

  // Primus Tracking UI ============================================================================
  // ===============================================================================================

  @Override
  public String getAmTrackingUrl() {
    return getPreflightAmTrackingUrl(
        applicationMeta.getPrimusConf(),
        applicationMeta.getApplicationId(),
        kubernetesNamespace,
        kubernetesDriverPodName);
  }

  public static String getPreflightAmTrackingUrl(
      PrimusConf primusConf,
      String appId,
      String kubernetesNamespace,
      String kubernetesDriverPodName
  ) {
    return Dictionary.newDictionary(
        appId,
        primusConf.getName(),
        kubernetesNamespace,
        kubernetesDriverPodName
    ).translate(primusConf
        .getRuntimeConf()
        .getKubernetesNativeConf()
        .getPrimusUiConf()
        .getTrackingUrlFormat());
  }

  @Override
  public String getHistoryTrackingUrl() {
    return getPreflightHistoryTrackingUrl(
        applicationMeta.getPrimusConf(),
        applicationMeta.getApplicationId(),
        kubernetesNamespace,
        kubernetesDriverPodName);
  }

  public static String getPreflightHistoryTrackingUrl(
      PrimusConf primusConf,
      String appId,
      String kubernetesNamespace,
      String kubernetesDriverPodName
  ) {
    return Dictionary.newDictionary(
        appId,
        primusConf.getName(),
        kubernetesNamespace,
        kubernetesDriverPodName
    ).translate(primusConf
        .getRuntimeConf()
        .getKubernetesNativeConf()
        .getPrimusUiConf()
        .getHistoryTrackingUrlFormat()
    );
  }

  // Logs ==========================================================================================
  // ===============================================================================================

  @Override
  public String getAmLogUrl() {
    return Dictionary
        .newDriverDictionary(applicationMeta, kubernetesNamespace, kubernetesDriverPodName)
        .translate(applicationMeta
            .getPrimusConf()
            .getRuntimeConf()
            .getKubernetesNativeConf()
            .getPrimusUiConf()
            .getContainerLogUrlFormat()
        );
  }

  @Override
  public String getAmHistoryLogUrl() {
    return Dictionary
        .newDriverDictionary(applicationMeta, kubernetesNamespace, kubernetesDriverPodName)
        .translate(applicationMeta.getPrimusConf()
            .getRuntimeConf()
            .getKubernetesNativeConf()
            .getPrimusUiConf()
            .getHistoryContainerLogUrlFormat()
        );
  }

  @Override
  public String getExecutorLogUrl(SchedulerExecutor schedulerExecutor) {
    return Dictionary
        .newExecutorDictionary(applicationMeta, kubernetesNamespace, schedulerExecutor)
        .translate(
            applicationMeta.getPrimusConf()
                .getRuntimeConf()
                .getKubernetesNativeConf()
                .getPrimusUiConf()
                .getContainerLogUrlFormat()
        );
  }

  @Override
  public String getExecutorHistoryLogUrl(SchedulerExecutor schedulerExecutor) {
    return Dictionary
        .newExecutorDictionary(applicationMeta, kubernetesNamespace, schedulerExecutor)
        .translate(
            applicationMeta.getPrimusConf()
                .getRuntimeConf()
                .getKubernetesNativeConf()
                .getPrimusUiConf()
                .getHistoryContainerLogUrlFormat()
        );
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
