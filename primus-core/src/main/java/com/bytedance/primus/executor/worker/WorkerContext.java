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

package com.bytedance.primus.executor.worker;

import com.bytedance.primus.api.records.ClusterSpec;
import com.bytedance.primus.common.child.ChildContext;
import java.util.HashMap;
import java.util.Map;

public class WorkerContext extends ChildContext {

  private ClusterSpec clusterSpec;
  private int maxRestartTimes;
  private int currentRestartTimes;

  public WorkerContext(String command, Map<String, String> environment,
      ClusterSpec clusterSpec, int maxRestartTimes) {
    Map<String, String> env = new HashMap<>(System.getenv());
    env.putAll(environment);
    super.setCommand(command);
    super.setEnvironment(env);
    this.clusterSpec = clusterSpec;
    this.maxRestartTimes = maxRestartTimes;
    this.currentRestartTimes = 0;
  }

  public ClusterSpec getClusterSpec() {
    return clusterSpec;
  }

  public boolean needRestart() {
    if (currentRestartTimes < maxRestartTimes) {
      return true;
    } else {
      return false;
    }
  }

  public void increaseRestartTimes() {
    ++currentRestartTimes;
  }
}
