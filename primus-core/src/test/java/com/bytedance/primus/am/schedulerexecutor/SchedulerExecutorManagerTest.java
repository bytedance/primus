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

package com.bytedance.primus.am.schedulerexecutor;

import static org.mockito.Mockito.when;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.api.protocolrecords.RegisterRequest;
import com.bytedance.primus.api.protocolrecords.impl.pb.RegisterRequestPBImpl;
import com.bytedance.primus.api.records.ExecutorSpec;
import com.bytedance.primus.api.records.impl.pb.EndpointPBImpl;
import com.bytedance.primus.api.records.impl.pb.ExecutorIdPBImpl;
import com.bytedance.primus.api.records.impl.pb.ExecutorSpecPBImpl;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusConfOuterClass.Scheduler;
import com.bytedance.primus.proto.PrimusRuntime.YarnScheduler;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchedulerExecutorManagerTest {

  @Mock
  private AMContext context;

  @Before
  public void init() {
    PrimusConf primusConf = PrimusConf.newBuilder()
        .setScheduler(Scheduler.newBuilder()
            .setYarnScheduler(YarnScheduler.newBuilder().build()).build())
        .build();
    when(context.getPrimusConf()).thenReturn(primusConf);
  }

  @Test
  public void testUpdateRegisterRequestWithPreviousSpec() {
    SchedulerExecutorManager suit = new SchedulerExecutorManager(context);

    RegisterRequest registerRequest = new RegisterRequestPBImpl();
    ExecutorIdPBImpl executorIdPB = getExecutorIdPB("worker-2", 1);
    registerRequest.setExecutorId(executorIdPB);
    registerRequest.setExecutorSpec(getExecutorSpec(executorIdPB, "10.0.0.1", 8081));

    ExecutorIdPBImpl previousPB = getExecutorIdPB("worker-1", 0);
    ExecutorSpec previousExecutorSpec = getExecutorSpec(previousPB, "10.0.0.1", 8080);
    suit.updateRegisterRequestWithPreviousSpec(registerRequest, previousExecutorSpec);

    Assert.assertEquals(8080, registerRequest.getExecutorSpec().getEndpoints().get(0).getPort());
    Assert.assertEquals("10.0.0.1", registerRequest.getExecutorSpec().getEndpoints().get(0).getHostname());
    Assert .assertEquals("worker-2", registerRequest.getExecutorSpec().getExecutorId().getRoleName());
    Assert.assertEquals(1, registerRequest.getExecutorSpec().getExecutorId().getUniqId());

  }

  private ExecutorSpec getExecutorSpec(
      ExecutorIdPBImpl executorIdPB, String hostname,
      int port) {
    ExecutorSpec executorSpec = new ExecutorSpecPBImpl();
    EndpointPBImpl endpointPB = new EndpointPBImpl();
    endpointPB.setHostname(hostname);
    endpointPB.setPort(port);
    executorSpec.setExecutorId(executorIdPB);
    executorSpec.setEndpoints(Arrays.asList(endpointPB));
    return executorSpec;
  }

  private ExecutorIdPBImpl getExecutorIdPB(String roleName, int uniqId) {
    ExecutorIdPBImpl executorId = new ExecutorIdPBImpl();
    executorId.setRoleName(roleName);
    executorId.setIndex(0);
    executorId.setUniqId(uniqId);
    return executorId;
  }

}
