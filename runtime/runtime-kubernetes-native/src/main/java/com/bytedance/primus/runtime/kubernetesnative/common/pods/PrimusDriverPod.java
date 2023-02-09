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

import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.DRIVER_API_SERVER_PORT;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.DRIVER_EXECUTOR_TRACKER_SERVICE_PORT;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.KUBERNETES_POD_META_LABEL_OWNER;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_APP_ID_ENV_KEY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_APP_ID_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_APP_NAME_ENV_KEY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_APP_NAME_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_DRIVER_POD_UNIQ_ID_ENV_KEY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_MOUNT_PATH;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_ROLE_DRIVER;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_ROLE_SELECTOR_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.SLEEP_SECONDS_BEFORE_POD_EXIT_ENV_KEY;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesContainerConstants.HADOOP_USER_NAME_ENV;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesContainerConstants.PRIMUS_LOCAL_MOUNTING_DIR_ENV;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesContainerConstants.PRIMUS_REMOTE_STAGING_DIR_ENV;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_SUBMIT_TIMESTAMP_ENV_KEY;

import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import com.bytedance.primus.proto.PrimusRuntime.KubernetesNativeConf;
import com.bytedance.primus.proto.PrimusRuntime.KubernetesPodConf;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.containers.PrimusDriverContainer;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.containers.PrimusInitContainer;
import com.bytedance.primus.runtime.kubernetesnative.common.utils.ResourceNameBuilder;
import com.bytedance.primus.runtime.kubernetesnative.runtime.dictionary.Dictionary;
import com.google.common.base.Preconditions;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.util.Config;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// The application master
public class PrimusDriverPod extends PrimusBasePod {

  private static final Logger LOG = LoggerFactory.getLogger(PrimusDriverPod.class);

  private final PrimusPodContext context;
  @Getter
  private V1Pod kubernetesPod;

  public PrimusDriverPod(
      PrimusPodContext context,
      KubernetesPodConf podConf
  ) throws IOException {
    super(podConf);

    this.context = context;

    // Setup Kubernetes Client
    // TODO: Make it configurable
    ApiClient client = Config.defaultClient();
    Configuration.setDefaultApiClient(client);
  }

  public void submit() {
    try {
      CoreV1Api api = new CoreV1Api();

      // Create DriverPod
      this.kubernetesPod = createDriverPod(
          api,
          context,
          getSharedVolumes(),
          getInitContainerMounts(),
          getMainContainerMounts());

      // Build OwnerReference
      V1OwnerReference driverPodOwnerReference = new V1OwnerReference()
          .name(Objects.requireNonNull(kubernetesPod.getMetadata()).getName())
          .apiVersion(kubernetesPod.getApiVersion())
          .uid(kubernetesPod.getMetadata().getUid())
          .kind(kubernetesPod.getKind());

      // Create Kubernetes ConfigMap
      createConfigMap(api, driverPodOwnerReference);

      // Create Kubernetes services to expose Primus AM services
      createDriverService(api, driverPodOwnerReference,
          context.getPrimusUiConf().getWebUiPort());

    } catch (ApiException ex) {
      LOG.error("Error when submitting pod: response={}", ex.getResponseBody());
      throw new PrimusRuntimeException(ex);
    }
  }

  private static V1Pod createDriverPod(
      CoreV1Api api,
      PrimusPodContext context,
      List<V1Volume> sharedVolumes,
      List<V1VolumeMount> initContainerMounts,
      List<V1VolumeMount> mainContainerMounts
  ) throws ApiException {
    // Preprocess
    String driverPodName = ResourceNameBuilder.buildDriverPodName(context.getApplicationId());
    LOG.info("AppId:" + context.getApplicationId() + ", DriverName:" + driverPodName);

    // Generate containers
    Preconditions.checkArgument(context.getRuntimeConf().hasKubernetesNativeConf());
    KubernetesNativeConf kubernetesNativeConf = context.getRuntimeConf().getKubernetesNativeConf();

    V1Container initContainer =
        new PrimusInitContainer(
            context.getApplicationId(),
            context.getHdfsStagingDir().toString(),
            kubernetesNativeConf.getDriverPodConf().getInitContainerConf(),
            context.getDriverEnvironMap(),
            initContainerMounts
        ).getKubernetesContainer();

    V1Container driverContainer =
        new PrimusDriverContainer(
            context.getApplicationId(),
            context.getResourceLimitMap(),
            kubernetesNativeConf.getDriverPodConf().getMainContainerConf(),
            context.getDriverEnvironMap(),
            mainContainerMounts
        ).getKubernetesContainer();

    // Assemble pod
    Dictionary dictionary = Dictionary.newDictionary(
        context.getApplicationId(),
        context.getAppName(),
        context.getBaseMeta().getKubernetesNamespace(),
        driverPodName
    );

    V1Pod raw = new V1Pod()
        .metadata(new V1ObjectMeta()
            .name(driverPodName)
            .namespace(context.getBaseMeta().getKubernetesNamespace())
            .labels(dictionary.translate(
                loadBaseLabelMap(context, driverPodName),
                kubernetesNativeConf.getDriverPodConf().getLabelsMap()
            ))
            .annotations(dictionary.translate(
                kubernetesNativeConf.getDriverPodConf().getAnnotationsMap()
            ))
        )
        .spec(new V1PodSpec()
            .serviceAccountName(context.getBaseMeta().getKubernetesServiceAccountName())
            .schedulerName(context.getBaseMeta().getKubernetesSchedulerName())
            .restartPolicy("Never")
            .volumes(sharedVolumes)
            .initContainers(Collections.singletonList(initContainer))
            .containers(Collections.singletonList(driverContainer)));

    LOG.info("Driver Pod to create: {}", raw);
    return api.createNamespacedPod(
        context.getBaseMeta().getKubernetesNamespace(),
        raw, "true" /* pretty */, null /* dryRun*/, null /* fieldManager */);
  }

  private V1ConfigMap createConfigMap(
      CoreV1Api api,
      V1OwnerReference ownerReference
  ) throws ApiException {
    V1ConfigMap raw = new V1ConfigMap()
        .metadata(new V1ObjectMeta()
            .name(ResourceNameBuilder.buildConfigMapName(context.getApplicationId()))
            .namespace(context.getBaseMeta().getKubernetesNamespace())
            .addOwnerReferencesItem(ownerReference))
        .data(loadConfigMapEnvs(context, ownerReference));

    LOG.info("ConfigMap to create: {}", raw);
    return api.createNamespacedConfigMap(
        context.getBaseMeta().getKubernetesNamespace(),
        raw, "true" /* pretty */, null /* dryRun*/, null /* fieldManager */);
  }

  private V1Service createDriverService(
      CoreV1Api api,
      V1OwnerReference ownerReference,
      int primusWebUiPort
  ) throws ApiException {
    V1Service raw = new V1Service()
        .metadata(new V1ObjectMeta()
            .name(ResourceNameBuilder.buildDriverShortServiceName(context.getApplicationId()))
            .addOwnerReferencesItem(ownerReference))
        .spec(new V1ServiceSpec()
            .clusterIP("None")
            .putSelectorItem(PRIMUS_APP_ID_LABEL_NAME, context.getApplicationId())
            .putSelectorItem(PRIMUS_ROLE_SELECTOR_LABEL_NAME, PRIMUS_ROLE_DRIVER)
            .ports(Arrays.asList(
                new V1ServicePort()
                    .name("api-service")
                    .port(DRIVER_API_SERVER_PORT),
                new V1ServicePort()
                    .name("executor-tracker-service")
                    .port(DRIVER_EXECUTOR_TRACKER_SERVICE_PORT),
                new V1ServicePort()
                    .name("web-server")
                    .port(primusWebUiPort)
            )));

    LOG.info("Service to create: {}", raw);
    return api.createNamespacedService(
        context.getBaseMeta().getKubernetesNamespace(), raw,
        "true" /* pretty */, null /* dryRun*/, null /* fieldManager */);
  }

  private static Map<String, String> loadConfigMapEnvs(
      PrimusPodContext context,
      V1OwnerReference ownerReference
  ) {
    // Primus envs
    Map<String, String> primusEnvs = new HashMap<String, String>() {{
      put(PRIMUS_APP_ID_ENV_KEY, context.getApplicationId());
      put(PRIMUS_APP_NAME_ENV_KEY, context.getAppName());

      put(PRIMUS_DRIVER_POD_UNIQ_ID_ENV_KEY, ownerReference.getUid());

      put(PRIMUS_SUBMIT_TIMESTAMP_ENV_KEY, String.valueOf(new Date().getTime()));
      put(PRIMUS_REMOTE_STAGING_DIR_ENV, context.getHdfsStagingDir().toString());
      put(PRIMUS_LOCAL_MOUNTING_DIR_ENV, PRIMUS_MOUNT_PATH);

      put(HADOOP_USER_NAME_ENV, context.getUser());
      put(SLEEP_SECONDS_BEFORE_POD_EXIT_ENV_KEY,
          Integer.toString(context.getSleepSecondsBeforePodExit()));
    }};

    // Assemble the result
    return new HashMap<String, String>() {{
      putAll(primusEnvs);
      putAll(context.getJobEnvironMap());
    }};
  }

  // TODO: Unit tests
  private static Map<String, String> loadBaseLabelMap(
      PrimusPodContext context,
      String masterPodName
  ) {
    return new HashMap<String, String>() {{
      // Pod common
      put(PRIMUS_APP_ID_LABEL_NAME, context.getApplicationId());
      put(PRIMUS_APP_NAME_LABEL_NAME, context.getAppName());
      put(KUBERNETES_POD_META_LABEL_OWNER, context.getUser());
      // Driver specific
      put(PRIMUS_ROLE_SELECTOR_LABEL_NAME, PRIMUS_ROLE_DRIVER);
    }};
  }

  public String getAMPodStatus() {
    try {
      CoreV1Api api = new CoreV1Api();
      String driverPodName = ResourceNameBuilder.buildDriverPodName(context.getApplicationId());
      V1Pod driverPod = api.readNamespacedPod(
          driverPodName,
          context.getBaseMeta().getKubernetesNamespace(),
          "true" /* pretty */,
          false /* exact */,
          false /* export */
      );
      return Objects.requireNonNull(driverPod.getStatus()).getPhase();

    } catch (Exception ex) {
      if (ex.getMessage().isEmpty() || // TODO: Retry instead of directly fail with empty msg.
          ex.getMessage().equalsIgnoreCase("Not Found")
      ) {
        LOG.error("Error when getAMPodStatus(), am pod is not existed.");
        return "Failed";
      }
      LOG.info("Error when get AM PodStatus:", ex);
    }
    return null;
  }
}
