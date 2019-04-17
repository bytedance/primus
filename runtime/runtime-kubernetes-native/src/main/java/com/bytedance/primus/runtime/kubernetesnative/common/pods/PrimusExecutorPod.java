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
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.KUBERNETES_POD_META_LABEL_PSM;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.KUBERNETES_POD_META_LABEL_PSM_DEFAULT;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_APP_SELECTOR_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_EXECUTOR_PRIORITY_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_EXECUTOR_SELECTOR_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_JOB_NAME_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_KUBERNETES_JOB_NAME_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_ROLE_SELECTOR_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesContainerConstants.PRIMUS_REMOTE_STAGING_DIR_ENV;

import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.proto.PrimusRuntime.KubernetesContainerConf;
import com.bytedance.primus.runtime.kubernetesnative.ResourceNameBuilder;
import com.bytedance.primus.runtime.kubernetesnative.SelectorNameBuilder;
import com.bytedance.primus.runtime.kubernetesnative.am.KubernetesAMContext;
import com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.containers.PrimusExecutorContainer;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.containers.PrimusInitContainer;
import com.bytedance.primus.runtime.kubernetesnative.utils.AnnotationUtil;
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
      KubernetesAMContext context,
      RoleInfo roleInfo,
      String executorPodName,
      KubernetesContainerConf initContainerConf,
      KubernetesContainerConf mainContainerConf,
      Map<String, String> environmentMap
  ) {
    // Preprocess
    super(initContainerConf, mainContainerConf);
    Preconditions.checkState(
        System.getenv().containsKey(PRIMUS_REMOTE_STAGING_DIR_ENV),
        "Missing PRIMUS_REMOTE_STAGING_DIR_ENV environment key!" /* errorMsg */);

    podName = executorPodName;
    priority = roleInfo.getPriority();

    // Create containers
    V1Container initContainer =
        new PrimusInitContainer(
            context.getAppName(),
            System.getenv().get(PRIMUS_REMOTE_STAGING_DIR_ENV),
            initContainerConf,
            getInitContainerMounts()
        ).getKubernetesContainer();

    V1Container executorContainer =
        new PrimusExecutorContainer(
            context.getAppName(),
            roleInfo,
            mainContainerConf,
            environmentMap,
            getMainContainerMounts()
        ).getKubernetesContainer();

    // Assemble pod
    String driverPodName = ResourceNameBuilder.buildDriverPodName(context.getAppName());
    String executorSelectorName = SelectorNameBuilder.buildExecutorNamePrefix(context.getAppName());
    kubernetesPod = new V1Pod()
        .metadata(new V1ObjectMeta()
            .name(executorPodName)
            .namespace(context.getKubernetesNamespace())
            .addOwnerReferencesItem(
                new V1OwnerReferenceBuilder()
                    .withName(driverPodName)
                    .withApiVersion("v1")
                    .withKind("Pod")
                    .withUid(context.getDriverPodUniqId())
                    .withController(true)
                    .build())
            .labels(new HashMap<String, String>() {{
              put(PRIMUS_APP_SELECTOR_LABEL_NAME, context.getAppName());
              put(PRIMUS_JOB_NAME_LABEL_NAME, context.getJobName());
              put(PRIMUS_KUBERNETES_JOB_NAME_LABEL_NAME, context.getKubernetesJobName());
              put(PRIMUS_ROLE_SELECTOR_LABEL_NAME, KubernetesConstants.PRIMUS_ROLE_EXECUTOR);
              put(KUBERNETES_POD_META_LABEL_OWNER, context.getUsername());
              put(KUBERNETES_POD_META_LABEL_PSM, KUBERNETES_POD_META_LABEL_PSM_DEFAULT);
              put(PRIMUS_EXECUTOR_PRIORITY_LABEL_NAME, Integer.toString(priority));
              put(PRIMUS_EXECUTOR_SELECTOR_LABEL_NAME, executorSelectorName);
            }})
            .annotations(AnnotationUtil.loadUserDefinedAnnotations(context)))
        .spec(
            new V1PodSpec()
                .schedulerName(context.getKubernetesSchedulerName())
                .restartPolicy("Never")
                .volumes(getSharedVolumes())
                .initContainers(Collections.singletonList(initContainer))
                .containers(Collections.singletonList(executorContainer)));
  }
}
