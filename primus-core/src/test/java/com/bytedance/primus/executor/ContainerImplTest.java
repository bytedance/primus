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

import static com.bytedance.primus.common.util.PrimusConstants.DEFAULT_SETUP_PORT_WITH_BATCH_OR_GANG_SCHEDULER_RETRY_MAX_TIMES;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bytedance.primus.executor.exception.PrimusExecutorException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
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
    ExecutorContext executorContext = new ExecutorContext(primusExecutorConf, null, null);
    container.setContext(executorContext);
    container.setFastestNetworkInterfaceAddress(InetAddress.getLocalHost());
    try {
      container.setupPortsForBatchSchedulerAndGangScheduler();
    } catch (PrimusExecutorException e) {
      Assertions.assertTrue(e.getMessage()
          .endsWith("retry:" + DEFAULT_SETUP_PORT_WITH_BATCH_OR_GANG_SCHEDULER_RETRY_MAX_TIMES));
    }
    serverSocket.close();
  }

  @Test
  public void testFindFirstValidInet4Address() {
    ContainerImpl container = new ContainerImpl();
    InetAddress invalidAddress = mock(InetAddress.class);
    List<InetAddress> list = Arrays.asList(invalidAddress);
    Assertions.assertNull(
        container.findFirstValidInet4Address("eth0", list),
        "should return null for 0.0.0.0");
  }
}
