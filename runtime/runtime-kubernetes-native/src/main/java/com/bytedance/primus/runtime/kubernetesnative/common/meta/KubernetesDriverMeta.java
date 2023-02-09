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

import com.bytedance.primus.am.PrimusApplicationMeta;
import com.bytedance.primus.runtime.kubernetesnative.common.utils.ResourceNameBuilder;
import lombok.Getter;

@Getter
public class KubernetesDriverMeta extends KubernetesBaseMeta {

  private final String driverPodUniqId;
  private final String driverHostName;
  private final String driverPodName;

  public KubernetesDriverMeta(PrimusApplicationMeta applicationMeta, String driverPodUniqId) {
    super(applicationMeta.getPrimusConf());

    this.driverPodUniqId = driverPodUniqId;
    this.driverHostName = ResourceNameBuilder
        .buildDriverServiceName(applicationMeta.getApplicationId(), getKubernetesNamespace());
    this.driverPodName = ResourceNameBuilder
        .buildDriverPodName(applicationMeta.getApplicationId());
  }
}
