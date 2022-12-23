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

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesPodStopper {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesPodStarter.class);
  private final CoreV1Api kubernetesCoreV1Api;

  public KubernetesPodStopper(ApiClient kubernetesApiClient) {
    this.kubernetesCoreV1Api = new CoreV1Api(kubernetesApiClient);
  }

  public void stopPod(String namespace, String podName) {
    try {
      V1Pod pod = kubernetesCoreV1Api
          .deleteNamespacedPod(
              podName,      // name
              namespace,    // namespace
              "true",       // pretty
              null,         // dryRun
              10,           // gracePeriodSeconds
              false,        // orphanDependents
              "Background", // propagationPolicy
              null          // body
          );
      LOG.info("Delete pod:{}, result: {}", podName, pod.getStatus());
    } catch (Exception ex) {
      LOG.warn("Failed to delete pod:" + podName, ex);
    }
  }
}
