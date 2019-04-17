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

import com.bytedance.primus.runtime.kubernetesnative.am.KubernetesAMContext;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Config;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesPodStopper {

  private static Logger LOG = LoggerFactory.getLogger(KubernetesPodStarter.class);
  ApiClient client = null; // TODO: Centralize to AM context

  public KubernetesPodStopper(KubernetesAMContext context) {
    try {
      client = Config.fromCluster();
      client.setHttpClient(
          client.getHttpClient().newBuilder()
              .protocols(context.getKubernetesApiProtocols())
              .build());

    } catch (IOException e) {
      System.err.println("Error when create K8s client");
      System.exit(-1);
    }
  }

  public void stopPod(String namespace, String podName) {
    try {
      CoreV1Api api = new CoreV1Api(client);
      V1Pod pod = api
          .deleteNamespacedPod(
              podName, // name
              namespace, // namespace
              "true", // pretty
              null, // dryRun
              10, // gracePeriodSeconds
              false, // orphanDependents
              "Background", // propagationPolicy
              null // body
          );
      LOG.info("Delete pod:{}, result: {}", podName, pod.getStatus());
    } catch (Exception ex) {
      LOG.warn("Failed to delete pod:" + podName, ex);
    }
  }


}
