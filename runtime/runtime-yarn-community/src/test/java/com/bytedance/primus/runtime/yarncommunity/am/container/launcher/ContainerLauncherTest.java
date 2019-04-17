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

package com.bytedance.primus.runtime.yarncommunity.am.container.launcher;

import static org.mockito.Mockito.when;

import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.impl.pb.ExecutorIdPBImpl;
import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceRequest;
import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceType;
import com.bytedance.primus.apiserver.proto.UtilsProto.ScheduleStrategy;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.apiserver.records.impl.RoleSpecImpl;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.runtime.yarncommunity.am.YarnAMContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ContainerLauncherTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Mock
  private YarnAMContext yarnAMContext;

  @Test
  public void testBuildExecutorCommand() throws IOException {
    InetSocketAddress rpcAddr = new InetSocketAddress(9090);
    when(yarnAMContext.getRpcAddress()).thenReturn(rpcAddr);
    PrimusConf primusConf = PrimusConf.newBuilder().build();
    when(yarnAMContext.getPrimusConf()).thenReturn(primusConf);
    Configuration conf = new YarnConfiguration();
    when(yarnAMContext.getHadoopConf()).thenReturn(conf);
    ContainerLauncher containerLauncher = new ContainerLauncher(yarnAMContext);
    RoleSpec roleSpec = new RoleSpecImpl();
    ExecutorId executorId = new ExecutorIdPBImpl();
    executorId.setUniqId(1);
    executorId.setRoleName("ps");
    executorId.setIndex(0);
    ResourceInformation resourceInformation = new ResourceInformation();
    roleSpec.getExecutorSpecTemplate().getResourceRequests();
    List<ResourceRequest> resourceRequests = new ArrayList<>();
    ResourceRequest request = ResourceRequest.newBuilder().setResourceType(ResourceType.VCORES)
        .setValue(1).build();
    resourceRequests.add(request);
    roleSpec.getExecutorSpecTemplate().setResourceRequests(resourceRequests);
    int delayStartSeconds = 2;
    roleSpec.setScheduleStrategy(
        ScheduleStrategy.newBuilder().setExecutorDelayStartSeconds(delayStartSeconds).build());

    RoleInfo roleInfo = new RoleInfo(Arrays.asList("ps"), "ps", roleSpec, 10);
    List<String> strings = containerLauncher
        .buildExecutorCommand(roleInfo, executorId);
    Assert.assertTrue(strings.get(0).contains(String.format("--delay=%d", delayStartSeconds)));
  }

}
