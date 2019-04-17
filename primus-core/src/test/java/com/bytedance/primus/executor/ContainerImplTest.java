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

package com.bytedance.primus.executor;

import static com.bytedance.primus.utils.PrimusConstants.DEFAULT_SETUP_PORT_WITH_BATCH_OR_GANG_SCHEDULER_RETRY_MAX_TIMES;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bytedance.primus.executor.exception.PrimusExecutorException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class ContainerImplTest {

  private static final Logger log = LoggerFactory.getLogger(ContainerImplTest.class);

  @Mock
  PrimusExecutorConf primusExecutorConf;

  @Test
  public void testSetupPortsForBatchSchedulerAndGangScheduler() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0, 0, InetAddress.getLocalHost());
    int localPort = serverSocket.getLocalPort();
    log.info("Create temp port: {}.", localPort);
    when(primusExecutorConf.getPortList()).thenReturn(Arrays.asList(Integer.toString(localPort)));
    ContainerImpl container = new ContainerImpl();
    ExecutorContext executorContext = new ExecutorContext(primusExecutorConf, null);
    container.setContext(executorContext);
    container.setFastestNetworkInterfaceAddress(InetAddress.getLocalHost());
    try {
      container.setupPortsForBatchSchedulerAndGangScheduler();
    } catch (PrimusExecutorException e) {
      Assert.assertTrue(e.getMessage()
          .endsWith("retry:" + DEFAULT_SETUP_PORT_WITH_BATCH_OR_GANG_SCHEDULER_RETRY_MAX_TIMES));
    }
    serverSocket.close();
  }

  @Test
  public void testFindFirstValidInet4Address() {
    ContainerImpl container = new ContainerImpl();
    InetAddress invalidAddress = mock(InetAddress.class);
    when(invalidAddress.getHostAddress()).thenReturn("0.0.0.0");
    List<InetAddress> list = Arrays.asList(invalidAddress);
    Assert.assertNull("should return null for 0.0.0.0",
        container.findFirstValidInet4Address("eth0", list));
  }

}
