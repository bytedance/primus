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

import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceRequest;
import com.bytedance.primus.apiserver.records.RoleSpec;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesResourceLimitConverter {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(KubernetesResourceLimitConverter.class);

  public static Map<String, String> buildResourceLimitMap(RoleSpec roleSpec) {
    Map<String, String> map = new HashMap<>();
    for (ResourceRequest request : roleSpec.getExecutorSpecTemplate().getResourceRequests()) {
      switch (request.getResourceType()) {
        case VCORES:
          map.put("cpu", Integer.toString(request.getValue()));
          break;
        case MEMORY_MB:
          map.put("memory", getMemoryLimitString(request.getValue()));
          break;
        case GPU: {
          map.put("nvidia.com/gpu", Integer.toString(request.getValue()));
          break;
        }
        case PORT: {
          LOGGER.warn("PORT is not supported");
          break;
        }
        default:
          LOGGER.warn("Unsupported resource type " + request.getResourceType());
      }
    }
    return map;
  }

  private static String getMemoryLimitString(int memory) {
    return memory + "Mi";
  }
}
