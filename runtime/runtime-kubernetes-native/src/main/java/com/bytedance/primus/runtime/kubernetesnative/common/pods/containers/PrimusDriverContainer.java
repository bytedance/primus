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

package com.bytedance.primus.runtime.kubernetesnative.common.pods.containers;

import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_DEFAULT_IMAGE_PULL_POLICY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_DRIVER_HOST_NAME_ENV_KEY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.RUNTIME_IDC_NAME_DEFAULT_VALUE;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.RUNTIME_IDC_NAME_KEY;

import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.proto.PrimusRuntime.KubernetesContainerConf;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;

public class PrimusDriverContainer extends PrimusBaseContainer {

  @Getter
  private final V1Container kubernetesContainer;

  public PrimusDriverContainer(
      String appName,
      String driverServiceName,
      Map<String, String> resourceLimitMap,
      KubernetesContainerConf containerConf,
      Map<String, String> environmentMap,
      List<V1VolumeMount> mainContainerMounts
  ) {
    kubernetesContainer = new V1Container()
        // Basic
        .name("primus-driver")
        .image(containerConf.getImageName())
        .imagePullPolicy(StringUtils.ensure(
            containerConf.getImagePullPolicy(),
            PRIMUS_DEFAULT_IMAGE_PULL_POLICY))
        .resources(getResourceRequirements(resourceLimitMap))
        // Env
        .addEnvFromItem(retrieveKubernetesConfigMap(appName))
        .env(combineEnvironmentVariables(
            // Primus defaults
            new HashMap<String, String>() {{
              put(PRIMUS_DRIVER_HOST_NAME_ENV_KEY, driverServiceName);
              put(RUNTIME_IDC_NAME_KEY, RUNTIME_IDC_NAME_DEFAULT_VALUE);
            }},
            // Customized envs
            environmentMap)
        )
        // Volumes
        .volumeMounts(mainContainerMounts)
        // Command
        .command(containerConf.getCommandList())
        .args(containerConf.getArgsList());
  }
}
