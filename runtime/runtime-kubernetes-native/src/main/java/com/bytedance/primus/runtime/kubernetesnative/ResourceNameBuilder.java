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

package com.bytedance.primus.runtime.kubernetesnative;

import com.google.common.base.Preconditions;

public class ResourceNameBuilder {

  public static String buildConfigMapName(String appName) {
    return appName + "-" + "config";
  }

  // Since other services such as GTS is directly manipulating primus jobs through the driver pod,
  // meanwhile they want the pod name assignable, we are using appName as the driver pod name.
  public static String buildDriverPodName(String appName) {
    return appName;
  }

  public static String buildDriverShortServiceName(String appName) {
    String driverServiceName = buildDriverPodName(appName) + "-" + "svc";
    return driverServiceName;
  }

  public static String buildDriverServiceName(String appName, String namespace) {
    String driverServiceName = buildDriverPodName(appName) + "-" + "svc." + namespace + ".svc";
    return driverServiceName;
  }

  public static String buildExecutorPodName(String appName, String executorUniqName) {
    Preconditions.checkArgument(appName.startsWith("primus"));
    String podName = appName + "-" + executorUniqName.replace("_", "-");
    return podName;
  }

}
