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

package com.bytedance.primus.runtime.kubernetesnative.client;

import com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants;
import com.bytedance.primus.utils.PrimusConstants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;

public abstract class Environment {

  // Local resources needed for Kubernetes Native (HDFS + KubeConfig + Container scripts)
  public static Path[] getKubernetesNativeLocalResourcePaths() {
    // Container scripts
    Path containerScriptDir = new Path(
        System.getenv(PrimusConstants.PRIMUS_SBIN_DIR_ENV_KEY),
        KubernetesConstants.CONTAINER_SCRIPT_DIR_PATH
    );

    ArrayList<Path> ret = Arrays.stream(new String[]{
            KubernetesConstants.CONTAINER_SCRIPT_START_DRIVER_FILENAME,
            KubernetesConstants.CONTAINER_SCRIPT_START_EXECUTOR_FILENAME,
        })
        .map(filename -> new Path(containerScriptDir, filename))
        .collect(Collectors.toCollection(ArrayList::new));

    return ret.toArray(new Path[]{});
  }
}
