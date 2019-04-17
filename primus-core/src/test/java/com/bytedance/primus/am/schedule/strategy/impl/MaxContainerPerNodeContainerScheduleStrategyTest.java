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

package com.bytedance.primus.am.schedule.strategy.impl;

import static org.mockito.Mockito.when;

import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.schedule.strategy.ContainerScheduleAction;
import com.bytedance.primus.am.schedule.strategy.ContainerScheduleContext;
import com.bytedance.primus.apiserver.proto.UtilsProto.ScheduleStrategy;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.apiserver.records.impl.RoleSpecImpl;
import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.model.records.NodeId;
import com.bytedance.primus.common.model.records.Priority;
import com.bytedance.primus.common.model.records.impl.pb.ContainerPBImpl;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MaxContainerPerNodeContainerScheduleStrategyTest {

  @Mock
  private RoleInfoManager roleInfoManager;

  @Test
  public void testProcessNewContainer() {
    Map<Integer, RoleInfo> roleInfoMap = new HashMap<>();
    RoleSpec roleSpec = new RoleSpecImpl();
    roleSpec.setScheduleStrategy(ScheduleStrategy.newBuilder().setMaxReplicasPerNode(1).build());
    RoleInfo roleInfo = new RoleInfo(Lists.newArrayList("BatchRole"), "BatchRole",
        roleSpec, 10);
    roleInfoMap.put(10, roleInfo);
    when(roleInfoManager.getPriorityRoleInfoMap()).thenReturn(roleInfoMap);
    MaxContainerPerNodeContainerScheduleStrategy strategy = new MaxContainerPerNodeContainerScheduleStrategy(
        roleInfoManager);
    Container container = new ContainerPBImpl();
    Priority priority = Priority.newInstance(10);
    container.setPriority(priority);

    NodeId nodeId = NodeId.newInstance("127.0.0.1", 1);
    container.setNodeId(nodeId);

    ContainerScheduleContext containerScheduleContext = new ContainerScheduleContext(container);

    ContainerScheduleAction action = strategy.processNewContainer(containerScheduleContext);
    Assert.assertEquals(ContainerScheduleAction.ACCEPT_CONTAINER, action);
    strategy.postProcess(action, containerScheduleContext);

    ContainerScheduleAction action2 = strategy.processNewContainer(containerScheduleContext);
    Assert.assertEquals(ContainerScheduleAction.RELEASE_CONTAINER, action2);

  }

}
