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
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesContainerConstants.PRIMUS_REMOTE_STAGING_DIR_ENV;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationMaster;
import com.bytedance.primus.am.PrimusApplicationMeta;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.proto.PrimusCommon;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.runtime.kubernetesnative.am.scheduler.KubernetesContainerManager;
import com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants;
import com.bytedance.primus.runtime.kubernetesnative.common.meta.KubernetesDriverMeta;
import com.bytedance.primus.runtime.kubernetesnative.runtime.monitor.MonitorInfoProviderImpl;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import okhttp3.Protocol;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KubernetesApplicationMaster extends ApplicationMaster {

  private static final int MAX_APP_ATTEMPTS = 1;

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesApplicationMaster.class);

  public KubernetesApplicationMaster(
      PrimusConf primusConf,
      String applicationId,
      String driverPodUniqId
  ) throws Exception {
    super(
        KubernetesApplicationMaster.class.getName(),
        createAMContext(primusConf, applicationId)
    );

    // Kubernetes components
    KubernetesDriverMeta driverMeta = new KubernetesDriverMeta(
        context.getApplicationMeta(),
        driverPodUniqId
    );

    ApiClient kubernetesApiClient = Config.fromCluster();
    kubernetesApiClient.setHttpClient(
        kubernetesApiClient.getHttpClient()
            .newBuilder()
            .protocols(getKubernetesClientProtocols(context.getApplicationMeta().getPrimusConf()))
            .build()
    );

    // Finalize ApplicationMaster initialization
    context.finalize(
        this,
        new MonitorInfoProviderImpl(context.getApplicationMeta()),
        new RoleInfoManager(context),
        new KubernetesContainerManager(context, driverMeta, kubernetesApiClient)
    );

    context
        .getCorePrimusServices()
        .forEach(this::addService);
  }

  private static AMContext createAMContext(
      PrimusConf primusConf,
      String applicationId
  ) throws Exception {

    LOG.info("Create PrimusApplicationMeta");
    Path stagingDir = new Path(System.getenv().get(PRIMUS_REMOTE_STAGING_DIR_ENV));

    String nodeId = "127.0.0.1:4444"; // TODO: Streamline the construction of forged ContainerId
    String username = StringUtils.ensure(
        primusConf.getRuntimeConf().getKubernetesNativeConf().getUser(),
        System.getenv().get("USER")
    );

    PrimusApplicationMeta applicationMeta = new PrimusApplicationMeta(
        primusConf,
        primusConf.getRuntimeConf().getKubernetesNativeConfOrBuilder().getPrimusUiConf(),
        username,
        applicationId,
        MAX_APP_ATTEMPTS,
        0, // Attempt is not supported in Kubernetes runtime
        stagingDir,
        KubernetesConstants.DRIVER_API_SERVER_PORT,
        nodeId
    );

    LOG.info("Create AMContext");
    return new AMContext(applicationMeta, DRIVER_EXECUTOR_TRACKER_SERVICE_PORT);
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
  protected void abort() {
    try {
      LOG.info("Abort application");
      saveHistory();
      stop();
      cleanup();

      // Tear down
      context.stop();

    } catch (Exception e) {
      LOG.warn("Failed to abort application", e);
    }
  }
}
