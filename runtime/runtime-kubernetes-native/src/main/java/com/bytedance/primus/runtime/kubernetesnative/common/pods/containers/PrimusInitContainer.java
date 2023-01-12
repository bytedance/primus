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

import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.proto.PrimusRuntime.KubernetesContainerConf;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

public class PrimusInitContainer extends PrimusBaseContainer {

  @Getter
  private final V1Container kubernetesContainer;

  public PrimusInitContainer(
      String appId,
      String stagingPath,
      KubernetesContainerConf containerConf,
      Map<String, String> envs,
      List<V1VolumeMount> mounts
  ) {
    // Input check
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(stagingPath),
        "staging path should not empty");

    // Build the container
    kubernetesContainer = new V1Container()
        // Basic
        .name("primus-init")
        .image(containerConf.getImageName())
        .imagePullPolicy(StringUtils.ensure(
            containerConf.getImagePullPolicy(),
            PRIMUS_DEFAULT_IMAGE_PULL_POLICY))
        // Env
        .addEnvFromItem(retrieveKubernetesConfigMap(appId))
        .env(envs.entrySet().stream()
            .map(pair -> new V1EnvVar()
                .name(pair.getKey())
                .value(pair.getValue()))
            .collect(Collectors.toList()))
        // Volumes
        .volumeMounts(mounts)
        // Command
        .command(containerConf.getCommandList())
        .args(containerConf.getArgsList());
  }
}
