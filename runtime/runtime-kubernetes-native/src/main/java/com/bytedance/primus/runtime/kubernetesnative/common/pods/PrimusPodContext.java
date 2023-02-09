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

import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_AM_JAVA_MEMORY_XMX;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_AM_JAVA_OPTIONS;
import static com.bytedance.primus.utils.PrimusConstants.SYSTEM_USER_ENV_KEY;

import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceRequest;
import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceType;
import com.bytedance.primus.apiserver.records.ExecutorSpec;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.apiserver.records.impl.ExecutorSpecImpl;
import com.bytedance.primus.apiserver.records.impl.RoleSpecImpl;
import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusConfOuterClass.Scheduler;
import com.bytedance.primus.proto.PrimusRuntime.PrimusUiConf;
import com.bytedance.primus.proto.PrimusRuntime.RuntimeConf;
import com.bytedance.primus.runtime.kubernetesnative.am.KubernetesResourceLimitConverter;
import com.bytedance.primus.runtime.kubernetesnative.common.meta.KubernetesBaseMeta;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.hadoop.fs.Path;

@Getter
public class PrimusPodContext {

  private final String applicationId;
  private final String appName;
  private final String user;
  private final Path hdfsStagingDir;
  private final Map<String, String> driverEnvironMap;
  private final Map<String, String> jobEnvironMap;  // use for config map
  private final int sleepSecondsBeforePodExit;
  private final RuntimeConf runtimeConf;
  protected final PrimusUiConf primusUiConf; // A pointer to PrimusUiConf in primusConf
  private final KubernetesBaseMeta baseMeta;
  private final Map<String, String> resourceLimitMap;

  public PrimusPodContext(
      String applicationId,
      Path stagingDir,
      PrimusConf primusConf
  ) {
    this.applicationId = applicationId;
    this.appName = primusConf.getName();
    this.hdfsStagingDir = stagingDir;
    this.user = StringUtils.ensure(
        primusConf.getRuntimeConf().getKubernetesNativeConf().getUser(),
        System.getenv().get(SYSTEM_USER_ENV_KEY));
    this.sleepSecondsBeforePodExit = primusConf
        .getRuntimeConf()
        .getKubernetesNativeConf()
        .getSleepSecondsBeforePodExit();
    this.baseMeta = new KubernetesBaseMeta(primusConf);
    this.jobEnvironMap = primusConf.getEnvMap();
    this.driverEnvironMap = getDriverStartEnvironment(primusConf);
    this.runtimeConf = primusConf.getRuntimeConf();
    this.primusUiConf = runtimeConf.getKubernetesNativeConf().getPrimusUiConf();
    this.resourceLimitMap = createResourceLimitMap(primusConf.getScheduler());
  }

  // TODO: Move it out of PrimusPodContext
  private static Map<String, String> getDriverStartEnvironment(PrimusConf conf) {
    Map<String, String> ret = new HashMap<String, String>() {{
      put(PRIMUS_AM_JAVA_MEMORY_XMX, conf.getScheduler().getJvmMemoryMb() + "m");
      putAll(conf.getScheduler().getEnvMap());
    }};

    if (!Strings.isNullOrEmpty(conf.getScheduler().getJavaOpts())) {
      ret.put(PRIMUS_AM_JAVA_OPTIONS, Integer.toString(conf.getScheduler().getJvmMemoryMb()));
    }

    return ret;
  }

  private static Map<String, String> createResourceLimitMap(Scheduler scheduler) {
    List<ResourceRequest> resourceRequests = new ArrayList<>();
    resourceRequests.add(
        ResourceRequest.newBuilder()
            .setResourceType(ResourceType.VCORES)
            .setValue(scheduler.getVcores())
            .build()
    );
    resourceRequests.add(
        ResourceRequest.newBuilder()
            .setResourceType(ResourceType.MEMORY_MB)
            .setValue(scheduler.getMemoryMb())
            .build()
    );
    if (scheduler.getGpuNum() != 0) {
      resourceRequests.add(
          ResourceRequest.newBuilder()
              .setResourceType(ResourceType.GPU)
              .setValue(scheduler.getGpuNum())
              .build());
    }

    ExecutorSpec executorSpec = new ExecutorSpecImpl();
    executorSpec.setResourceRequests(resourceRequests);

    RoleSpec roleSpec = new RoleSpecImpl();
    roleSpec.setExecutorSpecTemplate(executorSpec);
    return KubernetesResourceLimitConverter.buildResourceLimitMap(roleSpec);
  }
}
