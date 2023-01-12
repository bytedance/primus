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

package com.bytedance.primus.runtime.kubernetesnative.am;

import static com.bytedance.primus.runtime.kubernetesnative.am.scheduler.KubernetesContainerManager.FAKE_YARN_APPLICATION_ID;

import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.apiserver.client.models.Executor;
import com.bytedance.primus.apiserver.records.ExecutorSpec;
import com.bytedance.primus.apiserver.records.impl.ExecutorSpecImpl;
import com.bytedance.primus.apiserver.records.impl.MetaImpl;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.model.records.Priority;
import com.bytedance.primus.common.model.records.impl.pb.ContainerPBImpl;
import com.bytedance.primus.proto.PrimusRuntime.KubernetesNativeConf;
import com.bytedance.primus.runtime.kubernetesnative.am.scheduler.KubernetesContainerManager;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.KubernetesPodStarter;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.KubernetesPodStopper;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.PrimusExecutorPod;
import com.bytedance.primus.runtime.kubernetesnative.common.utils.ResourceNameBuilder;
import com.google.common.collect.ImmutableMap;
import io.kubernetes.client.openapi.ApiException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PodLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(PodLauncher.class);
  public static final String DEFAULT_EXECUTOR_PORT_RANGES = "9010";
  private final KubernetesAMContext context;
  private final AtomicLong index = new AtomicLong();
  private final KubernetesPodStarter executorPodStarter;
  private final KubernetesPodStopper executorPodStopper;

  public PodLauncher(KubernetesAMContext context) {
    this.context = context;
    executorPodStarter = new KubernetesPodStarter(context);
    executorPodStopper = new KubernetesPodStopper(context);
  }

  public void deleteOnePod(String podName) {
    executorPodStopper.stopPod(context.getKubernetesNamespace(), podName);
  }

  public PodLauncherResult createOnePod(RoleInfo roleInfo) {
    ExecutorId executorId = context
        .getSchedulerExecutorManager()
        .createExecutorForK8s(
            roleInfo,
            KubernetesContainerManager.FAKE_YARN_APPLICATION_ID,
            (id) -> ResourceNameBuilder.buildExecutorPodName(context.getAppId(), id)
        );

    // Register to Primus API server
    try {
      LOG.info("Creating executor pod:{}, index:{}. ", executorId, index.get());
      writeExecutorToApiServer(roleInfo, executorId);
    } catch (Exception e) {
      return PodLauncherResult.failed();
    }

    // Create PrimusExecutorPod
    KubernetesNativeConf runtimeConf = context.getPrimusConf()
        .getRuntimeConf()
        .getKubernetesNativeConf();

    PrimusExecutorPod executorPod = new PrimusExecutorPod(
        context,
        roleInfo,
        ResourceNameBuilder.buildExecutorPodName(
            context.getAppId(),
            executorId.toUniqString()),
        runtimeConf.getExecutorPodConf(),
        buildExecutorEnvironment(
            roleInfo,
            executorId)
    );

    Container container = new ContainerPBImpl();
    container.setId(ContainerId.newContainerId(
        FAKE_YARN_APPLICATION_ID,
        executorId.getUniqId()));
    container.setIsGuaranteed(true);
    container.setPriority(Priority.newInstance(executorPod.getPriority()));

    try {
      LOG.info("Starting executor pod: {}", executorPod.getKubernetesPod());
      executorPodStarter.startPod(context.getKubernetesNamespace(), executorPod.getKubernetesPod());
      PrimusMetrics.emitCounterWithAppIdTag(
          "am.pod_launcher.start_pod", new HashMap<>(), 1);
      PrimusMetrics.emitCounterWithAppIdTag(
          "am.container_launcher.start_container", new HashMap<>(), 1);
      return PodLauncherResult.succeed(container);

    } catch (ApiException e) {
      PrimusMetrics.emitCounterWithAppIdTag(
          "am.pod_launcher.start_pod_error", new HashMap<>(), 1);
      LOG.error(
          "error when start Pod: {}, reason: {}, err: {}",
          executorPod.getPodName(), e.getResponseBody(), e);
      return PodLauncherResult.failed();
    }
  }

  private Map<String, String> buildExecutorEnvironment(RoleInfo roleInfo, ExecutorId executorId) {
    ImmutableMap<String, String> envMap = ImmutableMap.<String, String>builder()
        .put("AM_HOST", context.getDriverHostName())
        .put("AM_PORT", Integer.toString(context.getRpcAddress().getPort()))
        .put("EXECUTOR_ROLE", executorId.getRoleName())
        .put("EXECUTOR_INDEX", Integer.toString(executorId.getIndex()))
        .put("EXECUTOR_UNIQ_ID", Long.toString(executorId.getUniqId()))
        .put("EXECUTOR_JAVA_OPTS", roleInfo.getRoleSpec().getExecutorSpecTemplate().getJavaOpts())
        .put("AM_APISERVER_HOST", context.getDriverHostName())
        .put("AM_APISERVER_PORT", Integer.toString(context.getApiServerPort()))
        .put("PORT_RANGES", DEFAULT_EXECUTOR_PORT_RANGES)
        .build();
    Map<String, String> mutableMap = new HashMap<>(envMap);
    mutableMap.putAll(roleInfo.getRoleSpec().getExecutorSpecTemplate().getEnvs());
    return mutableMap;
  }

  private void writeExecutorToApiServer(RoleInfo roleInfo, ExecutorId executorId) throws Exception {
    Executor executor = new Executor();
    executor.setMeta(new MetaImpl().setName(executorId.toUniqString()));
    ExecutorSpec executorSpec =
        new ExecutorSpecImpl(roleInfo.getRoleSpec().getExecutorSpecTemplate().getProto());
    executorSpec.setRoleIndex(executorId.getIndex());
    executor.setSpec(executorSpec);
    LOG.info("Write executor to api server: " + context.getCoreApi().createExecutor(executor));
  }
}
