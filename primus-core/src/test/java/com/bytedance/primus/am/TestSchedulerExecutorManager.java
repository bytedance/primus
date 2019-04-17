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

package com.bytedance.primus.am;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.model.records.NodeId;
import com.bytedance.primus.common.model.records.Priority;
import com.bytedance.primus.proto.PrimusConfOuterClass;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;

public class TestSchedulerExecutorManager {

  AMContext context;
  Map<Integer, Integer> priorityContainerIndexMap;
  PrimusConfOuterClass.Role role1;
  PrimusConfOuterClass.Role role2;

  @Before
  public void setup() {
    priorityContainerIndexMap = new HashMap<>();
    role1 = PrimusConfOuterClass.Role.newBuilder()
        .setRoleName("aaa")
        .setNum(2)
        .setVcores(1)
        .setMemoryMb(1)
        .setGpuNum(1)
        .setJvmMemoryMb(1)
        .setJavaOpts("java_opts_of_aaa")
        .setCommand("command_of_aaa")
        .putEnv("env_of_aaa_1", "value1")
        .putEnv("env_of_aaa_2", "value2")
        .setInputPolicy(PrimusConfOuterClass.InputPolicy.ENV)
        .setFailover(
            PrimusConfOuterClass.Failover.newBuilder()
                .setCommonFailoverPolicy(
                    PrimusConfOuterClass.CommonFailover.newBuilder()
                        .setRestartType(PrimusConfOuterClass.RestartType.ON_FAILURE)
                        .setMaxFailureTimes(2)
                        .setMaxFailurePolicy(PrimusConfOuterClass.MaxFailurePolicy.FAIL_ATTEMPT)
                        .build())
                .build())
        .setLocalRestartTimes(2)
        .build();

    role2 = PrimusConfOuterClass.Role.newBuilder()
        .setRoleName("bbb")
        .setNum(2)
        .setVcores(2)
        .setMemoryMb(2)
        .setGpuNum(2)
        .setJvmMemoryMb(2)
        .setJavaOpts("java_opts_of_bbb")
        .setCommand("command_of_bbb")
        .putEnv("env_of_bbb_1", "value1")
        .putEnv("env_of_bbb_2", "value2")
        .setInputPolicy(PrimusConfOuterClass.InputPolicy.ENV)
        .setFailover(
            PrimusConfOuterClass.Failover.newBuilder()
                .setCommonFailoverPolicy(
                    PrimusConfOuterClass.CommonFailover.newBuilder()
                        .setRestartType(PrimusConfOuterClass.RestartType.NEVER)
                        .setMaxFailureTimes(1)
                        .setMaxFailurePolicy(PrimusConfOuterClass.MaxFailurePolicy.FAIL_ATTEMPT)
                        .build())
                .build())
        .setLocalRestartTimes(2)
        .build();

  }
 
  private Container buildContainer(int priorityNum) {
    Priority priority = mock(Priority.class);
    when(priority.getPriority()).thenReturn(priorityNum);
    ContainerId containerId = mock(ContainerId.class);
    int index = priorityContainerIndexMap.getOrDefault(priorityNum, 0);
    priorityContainerIndexMap.put(priorityNum, index + 1);
    String containerIdStr = "container_" + priorityNum + "_" + index;
    when(containerId.toString()).thenReturn(containerIdStr);
    Container container = mock(Container.class);
    when(container.getPriority()).thenReturn(priority);
    when(container.getId()).thenReturn(containerId);
    NodeId nodeId = mock(NodeId.class);
    when(nodeId.getHost()).thenReturn("0.0.0.0");
    when(container.getNodeId()).thenReturn(nodeId);
    return container;
  }
}
