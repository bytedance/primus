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

import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.apiserver.proto.ResourceProto.ConsulConfig;
import com.bytedance.primus.executor.consul.model.ConsulEndpoint;
import com.bytedance.primus.executor.consul.service.ConsulManager;
import com.google.common.collect.Lists;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsulManagerImpl implements ConsulManager {

  private static final Logger log = LoggerFactory.getLogger(ConsulManagerImpl.class);

  public static final String YARN_INET_ADDR = "YARN_INET_ADDR";
  public static final String DEFAULT_YARN_NETWORK_ADDRESS = "";
  public static final int DEFAULT_SERVICE_TTL = 10;
  public static final String YARN_INET_ADDRESS_SPLITTER = ";";
  private String serviceName;
  private List<ServerSocket> serverSocketList;
  private ConsulConfig consulConfig;
  private ExecutorId executorId;
  private String[] ethernetAddressAddress;
  private Map<String, String> environment;
  private Random random;

  public ConsulManagerImpl(String serviceName, List<ServerSocket> serverSocketList,
      ConsulConfig consulConfig, ExecutorId executorId) {
    this(serviceName, serverSocketList, consulConfig, executorId, System.getenv(), new Random());
  }

  protected ConsulManagerImpl(String serviceName, List<ServerSocket> serverSocketList,
      ConsulConfig consulConfig, ExecutorId executorId, Map<String, String> environment, Random random) {
    this.serviceName = serviceName;
    this.serverSocketList = serverSocketList;
    this.consulConfig = consulConfig;
    this.executorId = executorId;
    this.environment = environment;
    this.random = random;
    init();
  }

  private void init() {
    createYarnEthernetAddressArray();
  }

  private void createYarnEthernetAddressArray() {
    ethernetAddressAddress = new String[serverSocketList.size()];
    if (environment.containsKey(YARN_INET_ADDR)) {
      String addr = environment.get(YARN_INET_ADDR);
      String[] addrs = addr.split(YARN_INET_ADDRESS_SPLITTER);
      log.info("YARN_INET_ADDR is: " + Arrays.toString(addrs));
      List<String> randomAddressList = shuffleIpAddress(Lists.newArrayList(addrs));
      for (int i = 0; i < ethernetAddressAddress.length; i++) {
        ethernetAddressAddress[i] = randomAddressList.get(i % randomAddressList.size());
      }
    }
    System.out.println(Arrays.toString(ethernetAddressAddress));

    for (int i = 0; i < ethernetAddressAddress.length; i++) {
      if (ethernetAddressAddress[i] == null) {
        ethernetAddressAddress[i] = DEFAULT_YARN_NETWORK_ADDRESS;
      }
    }
  }

  private List<String> shuffleIpAddress(List<String> addresses) {
    Collections.shuffle(addresses, random);
    return addresses;
  }

  @Override
  public List<ConsulEndpoint> computeConsulEndpoint() {
    int resisterCount = computeRegisterCount();
    log.info("total port count:{}, register port count:{}", getAllPortList().size(), resisterCount);
    List<ConsulEndpoint> consulEndpoints = new ArrayList<>();
    for (int i = 0; i < resisterCount; i++) {
      Map<String, String> basicTags = buildConsulTags();
      basicTags.put("PORT_INDEX", Integer.toString(i));

      ServerSocket serverSocket = serverSocketList.get(i);
      String newWorkAddress = ethernetAddressAddress[i];
      ConsulEndpoint consulEndpoint = new ConsulEndpoint(serviceName, newWorkAddress, serverSocket,
          basicTags, DEFAULT_SERVICE_TTL);
      consulEndpoints.add(consulEndpoint);
    }
    return consulEndpoints;
  }

  private int computeRegisterCount() {
    if (!consulConfig.hasPortRegisterNum()) {
      return serverSocketList.size();
    }
    return Math.min(serverSocketList.size(), consulConfig.getPortRegisterNum().getValue());
  }

  private Map<String, String> buildConsulTags() {
    Map<String, String> tags = new HashMap<>();
    tags.put("UNIQ_ID", executorId.toUniqString());
    tags.put("ROLE_INDEX", Integer.toString(executorId.getIndex()));
    tags.put("ALL_PORT", getAllPortList().toString());
    return tags;
  }

  private List<Integer> getAllPortList() {
    return serverSocketList.stream().map(serverSocket -> serverSocket.getLocalPort())
        .collect(Collectors.toList());
  }

}
