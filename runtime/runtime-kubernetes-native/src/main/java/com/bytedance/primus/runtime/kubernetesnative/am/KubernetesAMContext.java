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

import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.DRIVER_EXECUTOR_TRACKER_SERVICE_PORT;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.proto.PrimusCommon;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.runtime.kubernetesnative.common.KubernetesSchedulerConfig;
import com.bytedance.primus.runtime.kubernetesnative.common.utils.ResourceNameBuilder;
import com.bytedance.primus.runtime.kubernetesnative.runtime.monitor.MonitorInfoProviderImpl;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import okhttp3.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesAMContext extends AMContext {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesAMContext.class);

  @Getter
  @Setter
  private String apiServerHost;
  @Getter
  @Setter
  private int apiServerPort;

  @Getter
  private final String appId;
  @Getter
  private final String appName;
  @Getter
  private final String driverPodUniqId;
  @Getter
  private final String driverHostName;
  @Getter
  private final String driverPodName;
  @Getter
  private final String userName;
  @Getter
  private final List<Protocol> kubernetesApiProtocols;

  private final KubernetesSchedulerConfig kubernetesSchedulerConfig;

  public KubernetesAMContext(
      PrimusConf primusConf,
      String appId,
      String driverPodUniqId
  ) throws IOException {
    super(primusConf);

    this.appId = appId;
    this.driverPodUniqId = driverPodUniqId;

    this.appName = primusConf.getName();
    this.userName = primusConf
        .getRuntimeConf()
        .getKubernetesNativeConf()
        .getUser();

    this.kubernetesSchedulerConfig = new KubernetesSchedulerConfig(this.primusConf);
    this.kubernetesApiProtocols = getKubernetesClientProtocols(this.primusConf);

    driverHostName = ResourceNameBuilder.buildDriverServiceName(appId,
        kubernetesSchedulerConfig.getNamespace());
    driverPodName = ResourceNameBuilder.buildDriverPodName(appId);
    LOG.info("Driver Host Name:" + driverHostName + ", AppId:" + appId);

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
  public String getUsername() {
    return this.userName;
  }

  @Override
  public int getExecutorTrackPort() {
    return DRIVER_EXECUTOR_TRACKER_SERVICE_PORT;
  }

  public String getApplicationId() {
    return appId;
  }

  public int getAttemptId() {
    return 0; // Attempt is not supported in Kubernetes runtime.
  }

  public String getKubernetesNamespace() {
    return kubernetesSchedulerConfig.getNamespace();
  }

  public String getKubernetesSchedulerName() {
    return kubernetesSchedulerConfig.getSchedulerName();
  }
}
