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

package com.bytedance.primus.runtime.yarncommunity.am.container.scheduler.fair;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManager;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.apiserver.records.impl.RoleSpecImpl;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusConfOuterClass.Scheduler;
import com.bytedance.primus.runtime.yarncommunity.am.YarnAMContext;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FairContainerManagerTest {

  @Mock
  YarnAMContext context;

  @Mock
  RoleInfoManager roleInfoManager;

  @Mock
  SchedulerExecutorManager schedulerExecutorManager;

  @Mock
  AMRMClient<ContainerRequest> amClient;

  @Test
  public void testNormalDynamicContainerAllocate() {
    List<ContainerRequest> containerRequestsHolder = new ArrayList<>();
    doAnswer(invocation -> {
      ContainerRequest request = (ContainerRequest) invocation.getArguments()[0];
      containerRequestsHolder.add(request);
      return request;
    }).when(amClient).addContainerRequest(any());
    Map<Integer, RoleInfo> roleInfoMap = new HashMap<>();
    RoleSpec roleSpec = new RoleSpecImpl();
    roleSpec.setMinReplicas(1);
    roleSpec.setReplicas(3);
    FairRoleInfo roleInfo = new FairRoleInfo(Lists.newArrayList("FairRole"), "FairRole", roleSpec,
        0);
    roleInfoMap.putIfAbsent(roleInfo.getPriority(), roleInfo);

    when(roleInfoManager.getPriorityRoleInfoMap()).thenReturn(roleInfoMap);
    FairContainerManager fairContainerManager = new FairContainerManager(context);

    Map<Integer, ConcurrentSkipListSet<ContainerId>> priorityContainerIdsMap = new HashMap<>();
    priorityContainerIdsMap.putIfAbsent(0, new ConcurrentSkipListSet<>());
    fairContainerManager.setPriorityContainerIdsMap(priorityContainerIdsMap);
    fairContainerManager.setAmClient(amClient);
    fairContainerManager.getPriorityNeedRequestNumMap().putIfAbsent(0, 0);
    fairContainerManager.askForContainers();

    Assert.assertEquals(3, containerRequestsHolder.size());
    Assert.assertEquals(3,
        Integer.toUnsignedLong(fairContainerManager.getPriorityNeedRequestNumMap().get(0)));
  }

  @Before
  public void setUpContext() {
    Scheduler scheduler = Scheduler.newBuilder()
        .setMaxAllocationNumEachRound(10)
        .build();
    PrimusConf primusConf = PrimusConf.newBuilder()
        .setScheduler(scheduler)
        .build();
    when(context.getPrimusConf()).thenReturn(primusConf);
    when(context.getRoleInfoManager()).thenReturn(roleInfoManager);
    when(context.getSchedulerExecutorManager()).thenReturn(schedulerExecutorManager);
  }
}
