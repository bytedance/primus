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

import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_AM_POD_UNIQ_ID_ENV_KEY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_APP_NAME_ENV_KEY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_MOUNT_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_MOUNT_PATH;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_SUBMIT_TIMESTAMP_ENV_KEY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.SLEEP_SECONDS_BEFORE_POD_EXIT_ENV_KEY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesContainerConstants.HADOOP_USER_NAME_ENV;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesContainerConstants.PRIMUS_LOCAL_MOUNTING_DIR_ENV;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesContainerConstants.PRIMUS_REMOTE_STAGING_DIR_ENV;

import com.bytedance.primus.common.collections.Pair;
import com.bytedance.primus.proto.PrimusRuntime.KubernetesContainerConf;
import com.bytedance.primus.runtime.kubernetesnative.ResourceNameBuilder;
import com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1EmptyDirVolumeSource;
import io.kubernetes.client.openapi.models.V1HostPathVolumeSource;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

public abstract class PrimusBasePod {

  // Primus default mounts
  private static final List<V1Volume> defaultSharedVolumes =
      Collections.singletonList(
          new V1Volume()
              .name(PRIMUS_MOUNT_NAME)
              .emptyDir(new V1EmptyDirVolumeSource())
      );
  private static final List<V1VolumeMount> defaultSharedVolumeMounts =
      Collections.singletonList(
          new V1VolumeMount()
              .name(PRIMUS_MOUNT_NAME)
              .mountPath(KubernetesConstants.PRIMUS_MOUNT_PATH)
      );

  @Getter
  private final List<V1Volume> sharedVolumes;
  @Getter
  private final List<V1VolumeMount> initContainerMounts;
  @Getter
  private final List<V1VolumeMount> mainContainerMounts;

  public PrimusBasePod(
      KubernetesContainerConf initContainerConf,
      KubernetesContainerConf mainContainerConf
  ) {
    // Compute Volumes and VolumeMounts
    Map<String, Pair<String, String>> initMounts = loadMountMap(initContainerConf.getMountsMap());
    Map<String, Pair<String, String>> mainMounts = loadMountMap(mainContainerConf.getMountsMap());

    initContainerMounts = new ArrayList<V1VolumeMount>() {{
      addAll(defaultSharedVolumeMounts);
      addAll(loadVolumeMounts(mainMounts));
    }};
    mainContainerMounts = new ArrayList<V1VolumeMount>() {{
      addAll(defaultSharedVolumeMounts);
      addAll(loadVolumeMounts(mainMounts));
    }};
    sharedVolumes = new ArrayList<V1Volume>() {{
      addAll(defaultSharedVolumes);
      addAll(loadVolumes(initMounts, mainMounts));
    }};
  }

  protected static V1ConfigMap loadConfigMap(PrimusPodContext context) {
    // Primus envs
    Map<String, String> envs = new HashMap<String, String>() {{
      // Primus Application
      put(PRIMUS_APP_NAME_ENV_KEY, context.getAppName());
      put(PRIMUS_AM_POD_UNIQ_ID_ENV_KEY, context.getDriverPodUid());
      put(PRIMUS_SUBMIT_TIMESTAMP_ENV_KEY, String.valueOf(new Date().getTime()));
      put(SLEEP_SECONDS_BEFORE_POD_EXIT_ENV_KEY,
          Integer.toString(context.getSleepSecondsBeforePodExit()));

      put(PRIMUS_REMOTE_STAGING_DIR_ENV, context.getHdfsStagingDir().toString());
      put(PRIMUS_LOCAL_MOUNTING_DIR_ENV, PRIMUS_MOUNT_PATH);

      // TODO: Centralize to the future PrimusFileSystem interface
      put(HADOOP_USER_NAME_ENV, context.getOwner());
    }};

    // User defined envs
    envs.putAll(context.getJobEnvironMap());

    // Assemble the result
    return new V1ConfigMap()
        .metadata(new V1ObjectMeta()
            .name(ResourceNameBuilder.buildConfigMapName(context.getAppName()))
            .namespace(context.getKubernetesSchedulerConfig().getNamespace())
            .addOwnerReferencesItem(context.getDriverPodOwnerReference()))
        .data(envs);
  }

  // Transforms {HostPath -> MountPath} to {name -> (HostPath, MountPath)}
  private static Map<String, Pair<String, String>> loadMountMap(Map<String, String> map) {
    return map.entrySet().stream()
        .collect(Collectors.toMap(
            // TODO: Design a better id generator
            entry -> String.format("mount-%s-end",
                entry.getKey()
                    .replace("/", "--")
                    .replace(".", "--")
                    .replace("_", "--")
                    .toLowerCase()
            ),
            entry -> new Pair<>(entry.getKey(), entry.getValue())
        ));
  }

  // Transforms {name -> (HostPath, MountPath)} to [V1VolumeMount]
  private static List<V1VolumeMount> loadVolumeMounts(Map<String, Pair<String, String>> map) {
    return map.entrySet().stream()
        .map(entry -> new V1VolumeMount()
            .name(entry.getKey())
            .mountPath(entry.getValue().getValue())
            .readOnly(true))
        .collect(Collectors.toList());
  }

  // Combines and transform [{name -> (HostPath, MountPath)}] to [V1VolumeMount]
  private static List<V1Volume> loadVolumes(
      Map<String, Pair<String, String>> initContainerMap,
      Map<String, Pair<String, String>> mainContainerMap
  ) {
    Map<String, Pair<String, String>> combined =
        new HashMap<String, Pair<String, String>>() {{
          putAll(initContainerMap);
          putAll(mainContainerMap);
        }};

    return combined.entrySet().stream()
        .map(entry -> new V1Volume()
            .name(entry.getKey())
            .hostPath(new V1HostPathVolumeSource()
                .path(entry.getValue().getKey())
                .type("DirectoryOrCreate")))
        .collect(Collectors.toList());
  }
}
