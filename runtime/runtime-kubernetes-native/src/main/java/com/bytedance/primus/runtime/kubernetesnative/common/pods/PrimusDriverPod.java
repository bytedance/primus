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
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.OPERATOR_STATE_API_SERVER_PORT;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_APP_SELECTOR_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_DRIVER_SELECTOR_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_JOB_NAME_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_KUBERNETES_JOB_NAME_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_ROLE_DRIVER;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_ROLE_SELECTOR_LABEL_NAME;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.WEB_UI_SERVER_PORT;

import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import com.bytedance.primus.proto.PrimusRuntime.KubernetesContainerConf;
import com.bytedance.primus.runtime.kubernetesnative.ResourceNameBuilder;
import com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.containers.PrimusDriverContainer;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.containers.PrimusInitContainer;
import com.bytedance.primus.runtime.kubernetesnative.utils.AnnotationUtil;
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
  private final V1Pod kubernetesPod;

  public PrimusDriverPod(
      PrimusPodContext context,
      Map<String, String> resourceLimit,
      KubernetesContainerConf initContainerConf,
      KubernetesContainerConf mainContainerConf
  ) throws IOException {
    super(initContainerConf, mainContainerConf);

    this.context = context;
    this.kubernetesPod = createDriverPod(
        context,
        resourceLimit,
        getSharedVolumes(),
        getInitContainerMounts(),
        getMainContainerMounts());

    // Setup Kubernetes Client
    // TODO: Make it configurable
    ApiClient client = Config.defaultClient();
    Configuration.setDefaultApiClient(client);
  }

  public void submit() {
    try {
      CoreV1Api api = new CoreV1Api();

      // Create DriverPod
      V1Pod driverNamedPod = api.createNamespacedPod(
          context.getKubernetesSchedulerConfig().getNamespace(),
          kubernetesPod, "true" /* pretty */, null /* dryRun*/, null /* fieldManager */);
      context.setDriverPodUid(Objects.requireNonNull(driverNamedPod.getMetadata()).getUid());
      LOG.info("Driver Pod has been created: {}", driverNamedPod);

      // Build OwnerReference
      V1OwnerReference driverPodOwnerReference = new V1OwnerReference()
          .name(driverNamedPod.getMetadata().getName())
          .apiVersion(driverNamedPod.getApiVersion())
          .uid(driverNamedPod.getMetadata().getUid())
          .kind(driverNamedPod.getKind());
      context.setOwnerReference(driverPodOwnerReference);
      LOG.info("Kubernetes OwnerReference has been created: {}", driverPodOwnerReference);

      // Create Kubernetes ConfigMap
      V1ConfigMap configMap = api.createNamespacedConfigMap(
          context.getKubernetesSchedulerConfig().getNamespace(),
          loadConfigMap(context),
          "true" /* pretty */,
          null /* dryRun*/,
          null /* fieldManager */);
      LOG.info("Kubernetes ConfigMap has been created: {}", configMap);

      // Create Kubernetes services to expose Primus AM services
      V1Service service = createDriverService(api);
      LOG.info("Driver service has been created: {}", service);

    } catch (ApiException ex) {
      LOG.error("Error when submitting pod: response={}", ex.getResponseBody());
      throw new PrimusRuntimeException(ex);
    }
  }

  private static V1Pod createDriverPod(
      PrimusPodContext context,
      Map<String, String> resourceLimit,
      List<V1Volume> sharedVolumes,
      List<V1VolumeMount> initContainerMounts,
      List<V1VolumeMount> mainContainerMounts
  ) {
    // Preprocess
    String driverPodName = ResourceNameBuilder.buildDriverPodName(context.getAppName());
    String driverServiceName = ResourceNameBuilder.buildDriverServiceName(
        context.getAppName(),
        context.getKubernetesSchedulerConfig().getNamespace());
    LOG.info("AppName:" + context.getAppName() + ", DriverName:" + driverPodName);

    // Generate containers
    V1Container initContainer =
        new PrimusInitContainer(
            context.getAppName(),
            context.getHdfsStagingDir().toString(),
            context.getRuntimeConf().getKubernetesNativeConf().getInitContainerConf(),
            initContainerMounts
        ).getKubernetesContainer();

    V1Container driverContainer =
        new PrimusDriverContainer(
            context.getAppName(),
            driverServiceName,
            resourceLimit,
            context.getRuntimeConf().getKubernetesNativeConf().getDriverContainerConf(),
            context.getDriverEnvironMap(),
            mainContainerMounts
        ).getKubernetesContainer();

    // Assemble pod
    V1Pod ret = new V1Pod()
        .metadata(new V1ObjectMeta()
            .name(driverPodName)
            .namespace(context.getKubernetesSchedulerConfig().getNamespace())
            .annotations(AnnotationUtil.loadUserDefinedAnnotations(context))
            .labels(new HashMap<String, String>() {{
              put(PRIMUS_APP_SELECTOR_LABEL_NAME, context.getAppName());
              put(PRIMUS_JOB_NAME_LABEL_NAME, context.getJobName());
              put(PRIMUS_KUBERNETES_JOB_NAME_LABEL_NAME, context.getKubernetesJobName());
              put(PRIMUS_DRIVER_SELECTOR_LABEL_NAME, driverPodName);
              put(PRIMUS_ROLE_SELECTOR_LABEL_NAME, PRIMUS_ROLE_DRIVER);
              put(KubernetesConstants.KUBERNETES_POD_META_LABEL_OWNER, context.getOwner());
              put(KubernetesConstants.KUBERNETES_POD_META_LABEL_PSM, context.getPsm());
            }}))
        .spec(new V1PodSpec()
            .serviceAccountName(context.getKubernetesSchedulerConfig().getServiceAccountName())
            .schedulerName(context.getKubernetesSchedulerConfig().getSchedulerName())
            .restartPolicy("Never")
            .volumes(sharedVolumes)
            .initContainers(Collections.singletonList(initContainer))
            .containers(Collections.singletonList(driverContainer)));

    LOG.info("Driver pod settings: {}", ret.toString());
    return ret;
  }

  public String getAMPodStatus() {
    try {
      CoreV1Api api = new CoreV1Api();
      String driverPodName = ResourceNameBuilder.buildDriverPodName(context.getAppName());
      V1Pod driverPod = api
          .readNamespacedPod(driverPodName, context.getKubernetesSchedulerConfig().getNamespace(),
              "true" /* pretty */, false /* exact */, false /* export */);
      return Objects.requireNonNull(driverPod.getStatus()).getPhase();

    } catch (Exception ex) {
      if (ex.getMessage().equalsIgnoreCase("Not Found")) {
        LOG.error("Error when getAMPodStatus(), am pod is not existed.");
        return "Failed";
      }
      LOG.info("Error when get AM PodStatus:", ex);
    }
    return null;
  }

  private V1Service createDriverService(CoreV1Api api) throws ApiException {
    Preconditions.checkState(
        context.getDriverPodOwnerReference() != null /* expression */,
        "owner reference should not be null" /* error msg */);

    String driverName = ResourceNameBuilder.buildDriverPodName(context.getAppName());
    String driverServiceName = ResourceNameBuilder.buildDriverShortServiceName(
        context.getAppName());

    V1Service v1Service = new V1Service()
        .metadata(new V1ObjectMeta()
            .name(driverServiceName)
            .addOwnerReferencesItem(context.getDriverPodOwnerReference()))
        .spec(new V1ServiceSpec()
            .clusterIP("None")
            .putSelectorItem(PRIMUS_DRIVER_SELECTOR_LABEL_NAME, driverName)
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
                    .port(WEB_UI_SERVER_PORT),
                new V1ServicePort()
                    .name("operator-status-server")
                    .port(OPERATOR_STATE_API_SERVER_PORT))
            ));

    return api.createNamespacedService(
        context.getKubernetesSchedulerConfig().getNamespace(), v1Service,
        "true" /* pretty */, null /* dryRun*/, null /* fieldManager */);
  }

}
