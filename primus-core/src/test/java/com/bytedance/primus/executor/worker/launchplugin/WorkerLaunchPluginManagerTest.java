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

package com.bytedance.primus.executor.worker.launchplugin;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.impl.pb.ExecutorIdPBImpl;
import com.bytedance.primus.apiserver.proto.ResourceProto;
import com.bytedance.primus.apiserver.proto.ResourceProto.Plugin;
import com.bytedance.primus.apiserver.proto.ResourceProto.PluginConfig;
import com.bytedance.primus.apiserver.records.ExecutorSpec;
import com.bytedance.primus.apiserver.records.impl.ExecutorSpecImpl;
import com.bytedance.primus.common.child.ChildLaunchPlugin;
import com.bytedance.primus.common.collections.Pair;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.PrimusExecutorConf;
import com.bytedance.primus.executor.worker.WorkerContext;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusInput.InputManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WorkerLaunchPluginManagerTest {

  public static final String PONY_PLUGIN = "PonyPlugin";
  @Mock
  private ExecutorContext executorContext;

  @Mock
  private WorkerContext workerContext;

  @Test
  public void testFindPlugins() {
    PluginManager pluginManager = new PluginManager(executorContext, workerContext);
    PluginConfig pluginConfigWithoutVersion = PluginConfig.newBuilder()
        .addExtendPlugins(Plugin.newBuilder().setName(PONY_PLUGIN))
        .build();
    List<Pair<PluginIdentifier, ChildLaunchPlugin>> userSpecifiedPlugins = WorkerLaunchPluginManager
        .createPlugins(pluginManager, pluginConfigWithoutVersion.getExtendPluginsList());
    Assert.assertEquals(1, userSpecifiedPlugins.size());

    PluginConfig pluginConfigWithVersion = PluginConfig.newBuilder()
        .addExtendPlugins(Plugin.newBuilder().setName(PONY_PLUGIN).setVersion("1.0.0"))
        .build();
    userSpecifiedPlugins = WorkerLaunchPluginManager
        .createPlugins(pluginManager, pluginConfigWithVersion.getExtendPluginsList());
    Assert.assertEquals(1, userSpecifiedPlugins.size());

    PluginConfig pluginConfigWithWrongVersion = PluginConfig.newBuilder()
        .addExtendPlugins(Plugin.newBuilder().setName(PONY_PLUGIN).setVersion("0.0.0"))
        .build();

    userSpecifiedPlugins = WorkerLaunchPluginManager
        .createPlugins(pluginManager, pluginConfigWithWrongVersion.getExtendPluginsList());
    Assert.assertEquals(0, userSpecifiedPlugins.size());

  }

  @Test
  public void testFullFillPlugins() {
    ArrayList<ChildLaunchPlugin> emptyList = new ArrayList<>();
    PluginManager pluginManager = new PluginManager(executorContext, workerContext);
    PluginConfig pluginConfigWithoutVersion = PluginConfig.newBuilder()
        .addExtendPlugins(Plugin.newBuilder().setName(PONY_PLUGIN))
        .build();
    WorkerLaunchPluginManager.fullFillPlugins(emptyList, pluginManager, pluginConfigWithoutVersion);
    Assert.assertEquals(1, emptyList.size());

    WorkerLaunchPluginManager.fullFillPlugins(emptyList, pluginManager, pluginConfigWithoutVersion);
    Assert.assertEquals(1, emptyList.size());

    pluginConfigWithoutVersion = PluginConfig.newBuilder()
        .addExtendPlugins(Plugin.newBuilder().setName(PONY_PLUGIN))
        .addDisabledPlugins(Plugin.newBuilder().setName(PONY_PLUGIN).build())
        .build();

    ArrayList<ChildLaunchPlugin> ponyPluginWithDisabledPlugin = new ArrayList<>();
    WorkerLaunchPluginManager
        .fullFillPlugins(ponyPluginWithDisabledPlugin, pluginManager, pluginConfigWithoutVersion);
    Assert.assertEquals(0, ponyPluginWithDisabledPlugin.size());

  }

  @Test
  public void testFullFillSystemPlugins() {
    PrimusExecutorConf primusExecutorConf = mock(PrimusExecutorConf.class);
    InputManager inputManager = InputManager.newBuilder().setGracefulShutdown(false).build();
    when(primusExecutorConf.getPrimusConf()).thenReturn(PrimusConf.newBuilder().build());
    when(primusExecutorConf.getInputManager()).thenReturn(inputManager);

    PluginConfig pluginConfigWithoutVersion = PluginConfig.newBuilder()
        .build();
    ResourceProto.ExecutorSpec executorSpecProto = ResourceProto.ExecutorSpec.newBuilder()
        .setPluginConfig(pluginConfigWithoutVersion).build();
    ExecutorSpec executorSpec = new ExecutorSpecImpl(executorSpecProto);

    when(primusExecutorConf.getExecutorSpec()).thenReturn(executorSpec);
    when(executorContext.getPrimusConf()).thenReturn(primusExecutorConf);
    ExecutorId executorId = getExecutorId();
    when(executorContext.getExecutorId()).thenReturn(executorId);

    WorkerLaunchPluginChain workerLaunchPluginChain = WorkerLaunchPluginManager
        .getWorkerLaunchPluginChain(executorContext, workerContext);

    Assert.assertEquals(4, workerLaunchPluginChain.getWorkerLaunchPlugins().size());
    List<String> collect = workerLaunchPluginChain.getWorkerLaunchPlugins().stream()
        .map(t -> t.getClass().getName()).collect(Collectors.toList());
    List<String> expect = Arrays
        .asList("com.bytedance.primus.executor.worker.launchplugin.EnvPlugin",
            "com.bytedance.primus.executor.worker.launchplugin.SocketPlugin",
            "com.bytedance.primus.executor.worker.launchplugin.PrimusConfCompatiblePlugin",
            "com.bytedance.primus.executor.worker.launchplugin.TfConfigPlugin");
    Assert.assertEquals(expect, collect);

  }

  private ExecutorId getExecutorId() {
    ExecutorId executorId = new ExecutorIdPBImpl();
    executorId.setIndex(0);
    executorId.setRoleName("worker");
    executorId.setUniqId(0);
    return executorId;
  }

  @Test
  public void testDisableTfConfigFrameworkPlugins() {
    PrimusExecutorConf primusExecutorConf = mock(PrimusExecutorConf.class);
    InputManager inputManager = InputManager.newBuilder().setGracefulShutdown(false).build();
    when(primusExecutorConf.getPrimusConf()).thenReturn(PrimusConf.newBuilder().build());
    when(primusExecutorConf.getInputManager()).thenReturn(inputManager);

    PluginConfig pluginConfigWithoutVersion = PluginConfig.newBuilder()
        .addDisabledPlugins(Plugin.newBuilder().setName("TfConfigPlugin").build())
        .build();
    ResourceProto.ExecutorSpec executorSpecProto = ResourceProto.ExecutorSpec.newBuilder()
        .setPluginConfig(pluginConfigWithoutVersion).build();
    ExecutorSpec executorSpec = new ExecutorSpecImpl(executorSpecProto);

    when(primusExecutorConf.getExecutorSpec()).thenReturn(executorSpec);
    when(executorContext.getPrimusConf()).thenReturn(primusExecutorConf);
    ExecutorId executorId = getExecutorId();
    when(executorContext.getExecutorId()).thenReturn(executorId);

    WorkerLaunchPluginChain workerLaunchPluginChain = WorkerLaunchPluginManager
        .getWorkerLaunchPluginChain(executorContext, workerContext);

    Assert.assertEquals(3, workerLaunchPluginChain.getWorkerLaunchPlugins().size());
    List<String> collect = workerLaunchPluginChain.getWorkerLaunchPlugins().stream()
        .map(t -> t.getClass().getName()).collect(Collectors.toList());
    List<String> expect = Arrays
        .asList("com.bytedance.primus.executor.worker.launchplugin.EnvPlugin",
            "com.bytedance.primus.executor.worker.launchplugin.SocketPlugin",
            "com.bytedance.primus.executor.worker.launchplugin.PrimusConfCompatiblePlugin");
    Assert.assertEquals(expect, collect);
  }

}
