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

package com.bytedance.primus.runtime.kubernetesnative.runtime.monitor;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.runtime.kubernetesnative.am.KubernetesAMContext;
import com.bytedance.primus.runtime.monitor.MonitorInfoProvider;
import com.google.common.base.Preconditions;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorInfoProviderImpl implements MonitorInfoProvider {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorInfoProviderImpl.class);

  private static final String URL_FORMAT_KEY_PRIMUS_APP_NAME = "\\{\\{PrimusAppName\\}\\}";
  private static final String URL_FORMAT_KEY_KUBERNETES_NAMESPACE = "\\{\\{KubernetesNamespace\\}\\}";
  private static final String URL_FORMAT_KEY_KUBERNETES_POD_NAME = "\\{\\{KubernetesPodName\\}\\}";

  private final KubernetesAMContext context;

  public MonitorInfoProviderImpl(AMContext context) {
    Preconditions.checkArgument(
        context instanceof KubernetesAMContext,
        "KubernetesAMContext is required for KubernetesNative::MonitorInfoProviderImpl");

    this.context = (KubernetesAMContext) context;
  }

  @Override
  public String getApplicationId() {
    return context.getAppName();
  }

  @Override
  public int getAttemptId() {
    return 0; // Defaults to 0 as application attempt is not supported in Kubernetes runtime. 
  }

  @Override
  public String getExecutorNodeName(SchedulerExecutor schedulerExecutor) {
    return schedulerExecutor.getContainer().getNodeId().getHost();
  }


  private Map<String, String> getAmUrlFormatterDictionary() {
    return new HashMap<String, String>() {{
      put(URL_FORMAT_KEY_PRIMUS_APP_NAME, context.getAppName());
      put(URL_FORMAT_KEY_KUBERNETES_NAMESPACE, context.getKubernetesNamespace());
      put(URL_FORMAT_KEY_KUBERNETES_POD_NAME, context.getDriverPodName());
    }};
  }

  private Map<String, String> getExecutorUrlFormatterDictionary(SchedulerExecutor executor) {
    return new HashMap<String, String>() {{
      put(URL_FORMAT_KEY_PRIMUS_APP_NAME, context.getAppName());
      put(URL_FORMAT_KEY_KUBERNETES_NAMESPACE, context.getKubernetesNamespace());
      put(URL_FORMAT_KEY_KUBERNETES_POD_NAME, executor.getContainer().getNodeId().getHost());
    }};
  }

  // History file paths ============================================================================
  // ===============================================================================================

  @Override
  public String getHistorySnapshotSubdirectoryName() {
    return context.getAppName();
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
    return getPreflightAmTrackingUrl(
        context.getPrimusConf(),
        context.getAppName(),
        context.getKubernetesNamespace(),
        context.getDriverPodName());
  }

  public static String getPreflightAmTrackingUrl(
      PrimusConf primusConf,
      String appName,
      String kubernetesNamespace,
      String kubernetesDriverPodName
  ) {
    return StringUtils.genFromTemplateAndDictionary(
        primusConf
            .getRuntimeConf()
            .getKubernetesNativeConf()
            .getAmTrackingUrlFormat(),
        new HashMap<String, String>() {{
          put(URL_FORMAT_KEY_PRIMUS_APP_NAME, appName);
          put(URL_FORMAT_KEY_KUBERNETES_NAMESPACE, kubernetesNamespace);
          put(URL_FORMAT_KEY_KUBERNETES_POD_NAME, kubernetesDriverPodName);
        }}
    );
  }

  @Override
  public String getHistoryTrackingUrl() {
    return getPreflightHistoryTrackingUrl(
        context.getPrimusConf(),
        context.getAppName(),
        context.getKubernetesNamespace(),
        context.getDriverPodName());
  }

  public static String getPreflightHistoryTrackingUrl(
      PrimusConf primusConf,
      String appName,
      String kubernetesNamespace,
      String kubernetesDriverPodName
  ) {
    return StringUtils.genFromTemplateAndDictionary(
        primusConf
            .getRuntimeConf()
            .getKubernetesNativeConf()
            .getHistoryTrackingUrlFormat(),
        new HashMap<String, String>() {{
          put(URL_FORMAT_KEY_PRIMUS_APP_NAME, appName);
          put(URL_FORMAT_KEY_KUBERNETES_NAMESPACE, kubernetesNamespace);
          put(URL_FORMAT_KEY_KUBERNETES_POD_NAME, kubernetesDriverPodName);
        }}
    );
  }

  // Logs ==========================================================================================
  // ===============================================================================================

  @Override
  public String getAmLogUrl() {
    return StringUtils.genFromTemplateAndDictionary(
        context.getPrimusConf()
            .getRuntimeConf()
            .getKubernetesNativeConf()
            .getContainerLogUrlFormat(),
        getAmUrlFormatterDictionary()
    );
  }

  @Override
  public String getAmHistoryLogUrl() {
    return getAmLogUrl();
  }

  @Override
  public String getExecutorLogUrl(SchedulerExecutor schedulerExecutor) {
    return StringUtils.genFromTemplateAndDictionary(
        context.getPrimusConf()
            .getRuntimeConf()
            .getKubernetesNativeConf()
            .getContainerLogUrlFormat(),
        getExecutorUrlFormatterDictionary(schedulerExecutor)
    );
  }

  @Override
  public String getExecutorHistoryLogUrl(SchedulerExecutor schedulerExecutor) {
    return getExecutorLogUrl(schedulerExecutor);
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
