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

import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants;
import com.google.common.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import java.lang.reflect.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesPodStarter {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesPodStarter.class);
  private final ApiClient kubernetesApiClient;
  private final CoreV1Api kubernetesCoreV1Api;

  public KubernetesPodStarter(ApiClient kubernetesApiClient) {
    this.kubernetesApiClient = kubernetesApiClient;
    this.kubernetesCoreV1Api = new CoreV1Api(kubernetesApiClient);
  }

  public V1Pod startPod(String namespace, V1Pod pod) throws ApiException {
    LOG.info("Begin to Start POD:{}", pod.getMetadata().getName());
    V1Pod namespacedPod = kubernetesCoreV1Api
        .createNamespacedPod(
            namespace,
            pod,
            "true", // pretty
            null,   // dryRun
            null    // fieldManager
        );
    LOG.info("Successfully started POD:{}", pod.getMetadata().getName());
    return namespacedPod;
  }

  public Watch<V1Pod> createExecutorPodWatch(String namespace, String appId) {
    try {
      String labelSelector = KubernetesConstants.PRIMUS_APP_ID_LABEL_NAME + "=" + appId;
      Type watchType =
          new TypeToken<Response<V1Pod>>() {
          }.getType();

      return Watch.createWatch(
          kubernetesApiClient,
          // XXX: Inline to prevent mismatched type from shaded jar
          kubernetesCoreV1Api.listNamespacedPodCall(
              namespace,
              "true",        // pretty
              true,          // allowed watch bookmark
              null,          // continue
              null,          // fieldSelector => select return fields
              labelSelector, // selector
              50,            // limit
              null /* resourceVersion */, null /* resourceVersionMatch */, // Get the latest version
              1800,         // timeoutSeconds
              Boolean.TRUE, // watch
              null /* callback */
          ),
          watchType);

    } catch (ApiException e) {
      LOG.error("ERROR when call K8S API: {}", e.getResponseBody());
      throw new PrimusRuntimeException(e);
    }
  }
}
