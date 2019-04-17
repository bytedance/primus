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

import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.runtime.kubernetesnative.ResourceNameBuilder;
import com.bytedance.primus.runtime.kubernetesnative.am.KubernetesResourceLimitConverter;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1ConfigMapEnvSource;
import io.kubernetes.client.openapi.models.V1EnvFromSource;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class PrimusBaseContainer {

  // defaultEnvs has higher priorities.
  protected static List<V1EnvVar> combineEnvironmentVariables(
      Map<String, String> defaultEnvs,
      Map<String, String> customizedEnvs
  ) {
    Map<String, String> acc = new HashMap<String, String>() {{
      putAll(customizedEnvs);
      putAll(defaultEnvs);
    }};

    return acc.entrySet().stream().map(entry -> new V1EnvVar()
            .name(entry.getKey())
            .value(entry.getValue()))
        .collect(Collectors.toCollection(ArrayList::new));
  }

  protected static V1EnvFromSource retrieveKubernetesConfigMap(String appName) {
    return new V1EnvFromSource().configMapRef(
        new V1ConfigMapEnvSource().name(
            ResourceNameBuilder.buildConfigMapName(appName)));
  }

  protected static V1ResourceRequirements getResourceRequirements(RoleInfo roleInfo) {
    return getResourceRequirements(
        KubernetesResourceLimitConverter.buildResourceLimitMap(
            roleInfo.getRoleSpec()));
  }

  protected static V1ResourceRequirements getResourceRequirements(Map<String, String> limits) {
    V1ResourceRequirements ret = new V1ResourceRequirements();
    limits.forEach((key, value) -> ret.putLimitsItem(key, new Quantity(value)));
    return ret;
  }
}
