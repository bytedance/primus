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

package com.bytedance.primus.runtime.kubernetesnative.utils;

import com.bytedance.primus.runtime.kubernetesnative.am.KubernetesAMContext;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.PrimusPodContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

// TODO: Find a codegen tool
// TODO: Tests
public class AnnotationUtil {

  private static final String APP_NAME_TEMPLATE = "{{AppName}}";
  private static final String QUEUE_NAME_TEMPLATE = "{{QueueName}}";

  public static Map<String, String> loadUserDefinedAnnotations(PrimusPodContext context) {
    return loadUserDefinedAnnotations(
        context.getAppName(),
        context.getKubernetesSchedulerConfig().getQueue(),
        context
            .getRuntimeConf()
            .getKubernetesNativeConf()
            .getDriverContainerConf()
            .getAnnotationsMap());
  }

  public static Map<String, String> loadUserDefinedAnnotations(KubernetesAMContext context) {
    return loadUserDefinedAnnotations(
        context.getAppName(),
        context.getKubernetesQueueName(),
        context
            .getPrimusConf()
            .getRuntimeConf()
            .getKubernetesNativeConf()
            .getDriverContainerConf()
            .getAnnotationsMap());
  }

  private static Map<String, String> loadUserDefinedAnnotations(
      String kubernetesAppName,
      String kubernetesQueueName,
      Map<String, String> annotations
  ) {
    Map<String, String> dictionary = new HashMap<String, String>() {{
      put(APP_NAME_TEMPLATE, kubernetesAppName);
      put(QUEUE_NAME_TEMPLATE, kubernetesQueueName);
    }};

    return annotations
        .entrySet()
        .stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            entry -> dictionary.getOrDefault(entry.getValue(), entry.getValue())
        ));
  }
}
