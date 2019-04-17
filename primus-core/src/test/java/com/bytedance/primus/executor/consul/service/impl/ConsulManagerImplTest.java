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

package com.bytedance.primus.executor.consul.service.impl;

import static com.bytedance.primus.executor.consul.service.impl.ConsulManagerImpl.DEFAULT_SERVICE_TTL;
import static com.bytedance.primus.executor.consul.service.impl.ConsulManagerImpl.DEFAULT_YARN_NETWORK_ADDRESS;
import static com.bytedance.primus.executor.consul.service.impl.ConsulManagerImpl.YARN_INET_ADDR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.impl.pb.ExecutorIdPBImpl;
import com.bytedance.primus.apiserver.proto.ResourceProto.ConsulConfig;
import com.bytedance.primus.executor.consul.model.ConsulEndpoint;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Int32Value;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConsulManagerImplTest {

  private ExecutorId executorId;

  @Before
  public void init() {
    executorId = new ExecutorIdPBImpl();
    executorId.setIndex(0);
    executorId.setRoleName("ps");
    executorId.setUniqId(1);
  }

  @Test
  public void testCreateYarnEthernetAddressArray() {
    List<ServerSocket> serverSockets = new ArrayList<>();
    serverSockets.add(getPort(1001));
    serverSockets.add(getPort(1002));
    ConsulConfig consulConfigWithRegisterCount = getConsulConfigWithRegisterCount(1);
    ConsulManagerImpl consulManager = new ConsulManagerImpl("TEST_SERVICE", serverSockets,
        consulConfigWithRegisterCount, executorId);
    List<ConsulEndpoint> consulEndpoints = consulManager.computeConsulEndpoint();

    Assert.assertEquals(1, consulEndpoints.size());
    ConsulEndpoint consulEndpoint = consulEndpoints.get(0);
    Assert.assertEquals(DEFAULT_YARN_NETWORK_ADDRESS, consulEndpoint.getAddress());
    Assert.assertEquals("TEST_SERVICE", consulEndpoint.getServiceName());
    Assert.assertEquals(1001, consulEndpoint.getServerSocket().getLocalPort());
    Assert.assertEquals("0", consulEndpoint.getTags().get("ROLE_INDEX"));
    Assert.assertEquals("0", consulEndpoint.getTags().get("PORT_INDEX"));
    Assert.assertEquals("executor_ps_0_1", consulEndpoint.getTags().get("UNIQ_ID"));
    Assert.assertEquals(DEFAULT_SERVICE_TTL, consulEndpoint.getTtl());

    ConsulConfig consulConfigWithRegisterCountThree = getConsulConfigWithRegisterCount(3);
    consulManager = new ConsulManagerImpl("TEST_SERVICE", serverSockets,
        consulConfigWithRegisterCountThree, executorId);
    List<ConsulEndpoint> totalEndpoints = consulManager.computeConsulEndpoint();
    Assert.assertEquals(2, totalEndpoints.size());

    ConsulEndpoint lastEndpoint = totalEndpoints.stream().skip(totalEndpoints.size() - 1)
        .findFirst().get();
    Assert.assertEquals(1002, lastEndpoint.getServerSocket().getLocalPort());
    Assert.assertEquals("0", lastEndpoint.getTags().get("ROLE_INDEX"));
    Assert.assertEquals("1", lastEndpoint.getTags().get("PORT_INDEX"));
    Assert.assertEquals("executor_ps_0_1", lastEndpoint.getTags().get("UNIQ_ID"));
  }

  private ServerSocket getPort(int port){
    ServerSocket mock = mock(ServerSocket.class);
    when(mock.getLocalPort()).thenReturn(port);
    return mock;
  }

  @Test
  public void testYarnEthernetAddress() {
    List<ServerSocket> serverSockets = new ArrayList<>();
    serverSockets.add(getPort(1003));
    serverSockets.add(getPort(1004));
    serverSockets.add(getPort(1005));
    Random mockRandom = mock(Random.class);
    when(mockRandom.nextInt(2)).thenReturn(1);
    when(mockRandom.nextInt(1)).thenReturn(0);

    ConsulConfig consulConfigWithRegisterCount = getConsulConfigWithRegisterCount(3);
    Map<String, String> fakeEnv = ImmutableMap.of(YARN_INET_ADDR, "10.0.0.1;10.0.0.2");
    ConsulManagerImpl suit = new ConsulManagerImpl("Test", serverSockets,
        consulConfigWithRegisterCount, executorId, fakeEnv, mockRandom);
    List<ConsulEndpoint> consulEndpoints = suit.computeConsulEndpoint();
    System.out.println(consulEndpoints);

    Assert.assertEquals(3, consulEndpoints.size());

    ConsulEndpoint firstEndpoint = consulEndpoints.get(0);
    Assert.assertEquals(1003, firstEndpoint.getServerSocket().getLocalPort());
    Assert.assertEquals("10.0.0.1", firstEndpoint.getAddress());
    Assert.assertEquals("0", firstEndpoint.getTags().get("PORT_INDEX"));

    ConsulEndpoint second = consulEndpoints.get(1);
    Assert.assertEquals(1004, second.getServerSocket().getLocalPort());
    Assert.assertEquals("10.0.0.2", second.getAddress());
    Assert.assertEquals("1", second.getTags().get("PORT_INDEX"));

    ConsulEndpoint lastEndpoint = consulEndpoints.get(2);
    Assert.assertEquals(1005, lastEndpoint.getServerSocket().getLocalPort());
    Assert.assertEquals("10.0.0.1", lastEndpoint.getAddress());
    Assert.assertEquals("0", lastEndpoint.getTags().get("ROLE_INDEX"));
    Assert.assertEquals("2", lastEndpoint.getTags().get("PORT_INDEX"));
  }

  private ConsulConfig getConsulConfigWithRegisterCount(int count) {
    ConsulConfig consulConfig = ConsulConfig.newBuilder()
        .setPortRegisterNum(Int32Value.newBuilder()
            .setValue(count).build())
        .build();
    return consulConfig;
  }

}
