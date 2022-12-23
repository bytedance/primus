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

package com.bytedance.primus.runtime.kubernetesnative.common.pods;

import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.KUBERNETES_POD_META_LABEL_OWNER;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_APP_ID_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_APP_NAME_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_EXECUTOR_PRIORITY_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_ROLE_EXECUTOR;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_ROLE_SELECTOR_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesContainerConstants.PRIMUS_REMOTE_STAGING_DIR_ENV;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.PrimusApplicationMeta;
import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.proto.PrimusRuntime.KubernetesPodConf;
import com.bytedance.primus.runtime.kubernetesnative.common.meta.KubernetesDriverMeta;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.containers.PrimusExecutorContainer;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.containers.PrimusInitContainer;
import com.bytedance.primus.runtime.kubernetesnative.runtime.dictionary.Dictionary;
import com.google.common.base.Preconditions;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReferenceBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

public class PrimusExecutorPod extends PrimusBasePod {

  @Getter
  private final V1Pod kubernetesPod;
  @Getter
  private final String podName;
  @Getter
  private final int priority;

  public PrimusExecutorPod(
      AMContext context,
      KubernetesDriverMeta driverMeta,
      RoleInfo roleInfo,
      String executorPodName,
      KubernetesPodConf podConf,
      Map<String, String> environmentMap
  ) {
    // Preprocess
    super(podConf);
    Preconditions.checkState(
        System.getenv().containsKey(PRIMUS_REMOTE_STAGING_DIR_ENV),
        "Missing PRIMUS_REMOTE_STAGING_DIR_ENV environment key!" /* errorMsg */);

    podName = executorPodName;
    priority = roleInfo.getPriority();

    // Create containers
    V1Container initContainer =
        new PrimusInitContainer(
            context.getApplicationMeta().getApplicationId(),
            System.getenv().get(PRIMUS_REMOTE_STAGING_DIR_ENV),
            podConf.getInitContainerConf(),
            environmentMap,
            getInitContainerMounts()
        ).getKubernetesContainer();

    V1Container executorContainer =
        new PrimusExecutorContainer(
            context.getApplicationMeta().getApplicationId(),
            roleInfo,
            podConf.getMainContainerConf(),
            environmentMap,
            getMainContainerMounts()
        ).getKubernetesContainer();

    // Assemble pod
    Dictionary dictionary = Dictionary.newDictionary(
        context.getApplicationMeta().getApplicationId(),
        context.getApplicationMeta().getAppName(),
        driverMeta.getKubernetesNamespace(),
        executorPodName
    );

    kubernetesPod = new V1Pod()
        .metadata(new V1ObjectMeta()
            .name(executorPodName)
            .namespace(driverMeta.getKubernetesNamespace())
            .addOwnerReferencesItem(
                new V1OwnerReferenceBuilder()
                    .withName(driverMeta.getDriverPodName())
                    .withApiVersion("v1")
                    .withKind("Pod")
                    .withUid(driverMeta.getDriverPodUniqId())
                    .withController(true)
                    .build())
            .labels(dictionary.translate(
                loadBaseLabelMap(context.getApplicationMeta(), roleInfo.getPriority()),
                podConf.getLabelsMap()
            ))
            .annotations(dictionary.translate(
                podConf.getAnnotationsMap()
            )))
        .spec(
            new V1PodSpec()
                .schedulerName(driverMeta.getKubernetesSchedulerName())
                .restartPolicy("Never")
                .volumes(getSharedVolumes())
                .initContainers(Collections.singletonList(initContainer))
                .containers(Collections.singletonList(executorContainer)));
  }

  private static Map<String, String> loadBaseLabelMap(
      PrimusApplicationMeta applicationMeta,
      int priority
  ) {
    return new HashMap<String, String>() {{
      // Pod common
      put(PRIMUS_APP_ID_LABEL_NAME, applicationMeta.getApplicationId());
      put(PRIMUS_APP_NAME_LABEL_NAME, applicationMeta.getAppName());
      put(KUBERNETES_POD_META_LABEL_OWNER, applicationMeta.getUsername());
      // Executor specific
      put(PRIMUS_ROLE_SELECTOR_LABEL_NAME, PRIMUS_ROLE_EXECUTOR);
      put(PRIMUS_EXECUTOR_PRIORITY_LABEL_NAME, Integer.toString(priority));
    }};
  }
}
