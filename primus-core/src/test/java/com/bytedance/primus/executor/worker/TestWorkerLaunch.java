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

import static org.mockito.Mockito.mock;

import com.bytedance.primus.api.records.ClusterSpec;
import com.bytedance.primus.api.records.Endpoint;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.ExecutorSpec;
import com.bytedance.primus.api.records.impl.pb.ClusterSpecPBImpl;
import com.bytedance.primus.api.records.impl.pb.EndpointPBImpl;
import com.bytedance.primus.api.records.impl.pb.ExecutorIdPBImpl;
import com.bytedance.primus.api.records.impl.pb.ExecutorSpecPBImpl;
import com.bytedance.primus.common.event.Dispatcher;
import com.bytedance.primus.common.event.Event;
import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.PrimusExecutorConf;
import com.bytedance.primus.proto.PrimusConfOuterClass;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestWorkerLaunch {

  PrimusConfOuterClass.PrimusConf primusConf;
  PrimusConfOuterClass.Role role1;
  PrimusConfOuterClass.Role role2;

  @Before
  public void setup() throws IOException {
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
    primusConf = PrimusConfOuterClass.PrimusConf.newBuilder()
        .addRole(role1)
        .addRole(role2)
        .build();
  }

  @Test
  public void testWorkerSuccess() throws Exception {
    ServerSocket frameworkSocket = new ServerSocket(0);
    ExecutorId executorId = new ExecutorIdPBImpl();
    executorId.setRoleName("aa");
    executorId.setIndex(0);
    ExecutorContext executorContext = mock(ExecutorContext.class);
    PrimusExecutorConf primusExecutorConf = mock(PrimusExecutorConf.class);
    Mockito.when(primusExecutorConf.getPrimusConf()).thenReturn(primusConf);
    Mockito.when(executorContext.getPrimusConf()).thenReturn(primusExecutorConf);
    Mockito.when(executorContext.getExecutorId()).thenReturn(executorId);
    Mockito.when(executorContext.getFrameworkSocketList())
        .thenReturn(Collections.singletonList(frameworkSocket));
    Dispatcher dispatcher = mock(Dispatcher.class);
    EventHandler eventHandler = mock(EventHandler.class);
    Mockito.when(dispatcher.getEventHandler()).thenReturn(eventHandler);
    ArgumentCaptor<Event> captor =
        ArgumentCaptor.forClass(Event.class);
    String command = "echo \"ok\" && sleep 1";
    Map<String, String> env = new HashMap<>();
    env.put("key1", "value1");
    env.put("key2", "value2");
    env.put("CONTAINER_ID", "container_1000000000000_000000_01_000001");
    ClusterSpec clusterSpec1 = new ClusterSpecPBImpl();
    ExecutorSpec executorSpec1 = new ExecutorSpecPBImpl();
    ExecutorId executorId1 = new ExecutorIdPBImpl();
    executorId1.setRoleName("ps");
    executorId1.setIndex(1);
    executorId1.setUniqId(1);
    executorSpec1.setExecutorId(executorId1);
    Endpoint endpoint1 = new EndpointPBImpl();
    endpoint1.setHostname("hostname");
    endpoint1.setPort(12345);
    executorSpec1.setEndpoints(Collections.singletonList(endpoint1));
  }

  @Test
  public void testWorkerFailed() throws Exception {
    ServerSocket frameworkSocket = new ServerSocket(0);
    ExecutorId executorId = new ExecutorIdPBImpl();
    executorId.setRoleName("aa");
    executorId.setIndex(0);
    ExecutorContext executorContext = mock(ExecutorContext.class);
    PrimusExecutorConf primusExecutorConf = mock(PrimusExecutorConf.class);
    Mockito.when(primusExecutorConf.getPrimusConf()).thenReturn(primusConf);
    Mockito.when(executorContext.getPrimusConf()).thenReturn(primusExecutorConf);
    Mockito.when(executorContext.getExecutorId()).thenReturn(executorId);
    Mockito.when(executorContext.getFrameworkSocketList())
        .thenReturn(Collections.singletonList(frameworkSocket));
    Dispatcher dispatcher = mock(Dispatcher.class);
    EventHandler eventHandler = mock(EventHandler.class);
    Mockito.when(dispatcher.getEventHandler()).thenReturn(eventHandler);
    ArgumentCaptor<Event> captor =
        ArgumentCaptor.forClass(Event.class);
    String command = "echo \"ok\";exit 123";
    Map<String, String> env = new HashMap<>();
    env.put("key1", "value1");
    env.put("key2", "value2");
    env.put("CONTAINER_ID", "container_1560910984532_581980_01_000001");
    ClusterSpec clusterSpec1 = new ClusterSpecPBImpl();
    ExecutorSpec executorSpec1 = new ExecutorSpecPBImpl();
    ExecutorId executorId1 = new ExecutorIdPBImpl();
    executorId1.setRoleName("ps");
    executorId1.setIndex(1);
    executorId1.setUniqId(1);
    executorSpec1.setExecutorId(executorId1);
    Endpoint endpoint1 = new EndpointPBImpl();
    endpoint1.setHostname("hostname.org");
    endpoint1.setPort(19153);
    executorSpec1.setEndpoints(Collections.singletonList(endpoint1));
  }

  @Test
  public void testKill() throws Exception {
    ServerSocket frameworkSocket = new ServerSocket(0);
    ExecutorId executorId = new ExecutorIdPBImpl();
    executorId.setRoleName("aa");
    executorId.setIndex(0);
    ExecutorContext executorContext = mock(ExecutorContext.class);
    PrimusExecutorConf primusExecutorConf = mock(PrimusExecutorConf.class);
    Mockito.when(primusExecutorConf.getPrimusConf()).thenReturn(primusConf);
    Mockito.when(executorContext.getPrimusConf()).thenReturn(primusExecutorConf);
    Mockito.when(executorContext.getExecutorId()).thenReturn(executorId);
    Mockito.when(executorContext.getFrameworkSocketList())
        .thenReturn(Collections.singletonList(frameworkSocket));
    Dispatcher dispatcher = mock(Dispatcher.class);
    EventHandler eventHandler = mock(EventHandler.class);
    Mockito.when(dispatcher.getEventHandler()).thenReturn(eventHandler);
    ArgumentCaptor<Event> captor =
        ArgumentCaptor.forClass(Event.class);
    String command = "echo \"start sleep\";sleep 1000; echo \"finish sleep\";exit 123";
    Map<String, String> env = new HashMap<>();
    env.put("key1", "value1");
    env.put("key2", "value2");
    env.put("CONTAINER_ID", "container_1560910984532_581980_01_000001");
    ClusterSpec clusterSpec1 = new ClusterSpecPBImpl();
    ExecutorSpec executorSpec1 = new ExecutorSpecPBImpl();
    ExecutorId executorId1 = new ExecutorIdPBImpl();
    executorId1.setRoleName("ps");
    executorId1.setIndex(1);
    executorId1.setUniqId(1);
    executorSpec1.setExecutorId(executorId1);
    Endpoint endpoint1 = new EndpointPBImpl();
    endpoint1.setHostname("hostname.org");
    endpoint1.setPort(19153);
    executorSpec1.setEndpoints(Collections.singletonList(endpoint1));
  }

  @Test
  public void testOutputStream() throws Exception {
    // TODO: Test output stream for subprocess
  }
}
