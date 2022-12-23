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

package com.bytedance.primus.runtime.kubernetesnative.common.meta;

import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.K8S_SCHEDULE_QUEUE_NAME_ANNOTATION_VALUE_DEFAULT;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.K8S_SCHEDULE_SCHEDULER_NAME_ANNOTATION_VALUE_DEFAULT;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.K8S_SCHEDULE_SERVICE_ACCOUNT_NAME_DEFAULT;

import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusRuntime.KubernetesNativeConf;
import com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants;
import lombok.Getter;

@Getter
public class KubernetesBaseMeta {

  private final String kubernetesQueue; // TODO: Deprecate
  private final String kubernetesNamespace;
  private final String kubernetesServiceAccountName;
  private final String kubernetesSchedulerName;

  public KubernetesBaseMeta(PrimusConf primusConf) {
    KubernetesNativeConf runtimeConf = primusConf
        .getRuntimeConf()
        .getKubernetesNativeConf();

    kubernetesQueue = StringUtils.ensure(
        primusConf.getQueue(),
        K8S_SCHEDULE_QUEUE_NAME_ANNOTATION_VALUE_DEFAULT);
    kubernetesNamespace = StringUtils.ensure(
        runtimeConf.getNamespace(),
        KubernetesConstants.PRIMUS_DEFAULT_K8S_NAMESPACE);
    kubernetesServiceAccountName = StringUtils.ensure(
        runtimeConf.getServiceAccount(),
        K8S_SCHEDULE_SERVICE_ACCOUNT_NAME_DEFAULT);
    kubernetesSchedulerName = StringUtils.ensure(
        runtimeConf.getSchedulerName(),
        K8S_SCHEDULE_SCHEDULER_NAME_ANNOTATION_VALUE_DEFAULT);
  }
}
