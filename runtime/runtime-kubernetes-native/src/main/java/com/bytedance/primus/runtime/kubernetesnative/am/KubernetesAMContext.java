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

package com.bytedance.primus.runtime.kubernetesnative.am;

import static com.bytedance.primus.am.ApplicationExitCode.KUBERNETES_ENV_MISSING;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.DRIVER_API_SERVER_PORT;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.DRIVER_EXECUTOR_TRACKER_SERVICE_PORT;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_AM_POD_UNIQ_ID_ENV_KEY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_APP_NAME_ENV_KEY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_DRIVER_HOST_NAME_ENV_KEY;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.model.records.NodeId;
import com.bytedance.primus.common.model.records.Priority;
import com.bytedance.primus.common.model.records.impl.pb.ContainerPBImpl;
import com.bytedance.primus.proto.PrimusCommon;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.runtime.kubernetesnative.ResourceNameBuilder;
import com.bytedance.primus.runtime.kubernetesnative.am.scheduler.KubernetesContainerManager;
import com.bytedance.primus.runtime.kubernetesnative.common.KubernetesSchedulerConfig;
import com.bytedance.primus.runtime.kubernetesnative.runtime.monitor.MonitorInfoProviderImpl;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import okhttp3.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesAMContext extends AMContext {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesAMContext.class);

  private static final int defaultAPIServerPort = DRIVER_API_SERVER_PORT;
  private static final int k8sDefaultExecutorTrackPort = DRIVER_EXECUTOR_TRACKER_SERVICE_PORT;

  private String apiServerHost;
  private int apiServerPort;
  private String appName;
  private String driverHostName;
  private String driverPodName;
  private String driverPodUniqId;
  private String jobName;
  private String kubernetesJobName;
  private String userName;

  private final KubernetesSchedulerConfig kubernetesSchedulerConfig;
  @Getter
  private final List<Protocol> kubernetesApiProtocols;

  public KubernetesAMContext(PrimusConf primusConf) {
    super(primusConf);

    if (System.getenv().containsKey(PRIMUS_APP_NAME_ENV_KEY)) {
      appName = System.getenv().get(PRIMUS_APP_NAME_ENV_KEY);
      driverHostName = System.getenv().get(PRIMUS_DRIVER_HOST_NAME_ENV_KEY);
      driverPodName = ResourceNameBuilder.buildDriverPodName(appName);
      LOG.info("Driver Host Name:" + driverHostName + ", AppName:" + appName);
    } else {
      LOG.error("Missing Environment Key:" + PRIMUS_APP_NAME_ENV_KEY);
      System.exit(KUBERNETES_ENV_MISSING.getValue());
    }
    if (System.getenv().containsKey(PRIMUS_AM_POD_UNIQ_ID_ENV_KEY)) {
      driverPodUniqId = System.getenv().get(PRIMUS_AM_POD_UNIQ_ID_ENV_KEY);
    } else {
      LOG.error("Missing Environment Key:" + PRIMUS_AM_POD_UNIQ_ID_ENV_KEY);
      System.exit(KUBERNETES_ENV_MISSING.getValue());
    }
    this.jobName = primusConf.getName();
    this.userName = primusConf.getKubernetesJobConf().getOwner();
    this.kubernetesJobName = primusConf.getKubernetesJobConf().getKubernetesJobName();

    this.kubernetesSchedulerConfig = new KubernetesSchedulerConfig(this.primusConf);
    this.kubernetesApiProtocols = getKubernetesClientProtocols(this.primusConf);

    super.init(new MonitorInfoProviderImpl(this));
  }

  private static List<Protocol> getKubernetesClientProtocols(PrimusConf primusConf) {
    List<Protocol> protocols = primusConf
        .getRuntimeConf()
        .getKubernetesNativeConf()
        .getKubernetesApiProtocolsList().stream().map(protocol -> {
          Map<PrimusCommon.Protocol, Protocol> map =
              new HashMap<PrimusCommon.Protocol, Protocol>() {{
                put(PrimusCommon.Protocol.HTTP_1, Protocol.HTTP_1_1);
                put(PrimusCommon.Protocol.HTTP_2, Protocol.HTTP_2);
              }};
          return map.getOrDefault(protocol, null);
        })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    return protocols.isEmpty()
        ? Collections.singletonList(Protocol.HTTP_2)
        : protocols;
  }

  @Override
  public ContainerId getContainerId() {
    ContainerId containerId = ContainerId
        .newContainerId(KubernetesContainerManager.FAKE_YARN_APPLICATION_ID, 0);
    Container container = new ContainerPBImpl();
    container.setId(containerId);
    container.setPriority(Priority.newInstance(9));
    container.setNodeHttpAddress("");
    container.setNodeId(NodeId.newInstance("127.0.0.1", 0));
    if (container.getNodeId().getHost() != null) {
      nodeId = container.getNodeId().getHost() + ":" + 4444;
    }
    return containerId;
  }

  @Override
  public String getUsername() {
    return this.userName;
  }

  public String getApiServerHost() {
    return apiServerHost;
  }

  public void setApiServerHost(String apiServerHost) {
    this.apiServerHost = apiServerHost;
  }

  public int getApiServerPort() {
    return apiServerPort;
  }

  public void setApiServerPort(int apiServerPort) {
    this.apiServerPort = apiServerPort;
  }

  public String getAppName() {
    return appName;
  }


  public String getDriverHostName() {
    return driverHostName;
  }

  public String getDriverPodUniqId() {
    return driverPodUniqId;
  }

  @Override
  public int getExecutorTrackPort() {
    return k8sDefaultExecutorTrackPort;
  }

  public int getDefaultAPIServerPort() {
    return defaultAPIServerPort;
  }

  public String getDriverPodName() {
    Preconditions
        .checkState(!Strings.isNullOrEmpty(driverPodName), "driverPodName should not be null!");
    return driverPodName;
  }

  public String getJobName() {
    return jobName;
  }

  public String getKubernetesJobName() {
    return kubernetesJobName;
  }

  public String getApplicationNameForStorage() {
    return getAppName();
  }

  public String getKubernetesNamespace() {
    return kubernetesSchedulerConfig.getNamespace();
  }

  public String getKubernetesSchedulerName() {
    return kubernetesSchedulerConfig.getSchedulerName();
  }

  public String getKubernetesQueueName() {
    return kubernetesSchedulerConfig.getQueue();
  }
}
