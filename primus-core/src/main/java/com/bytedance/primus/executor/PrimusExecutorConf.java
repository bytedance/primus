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

import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.impl.pb.ExecutorIdPBImpl;
import com.bytedance.primus.apiserver.client.Client;
import com.bytedance.primus.apiserver.client.DefaultClient;
import com.bytedance.primus.apiserver.client.apis.CoreApi;
import com.bytedance.primus.apiserver.records.ExecutorSpec;
import com.bytedance.primus.executor.exception.PrimusExecutorException;
import com.bytedance.primus.proto.PrimusCommon.RunningMode;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusInput.InputManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrimusExecutorConf {

  private static final Logger LOG = LoggerFactory.getLogger(PrimusExecutorConf.class);

  private String amHost;
  private int amPort;
  private ExecutorId executorId;

  protected final PrimusConf primusConf;

  private CoreApi coreApi;
  private ExecutorSpec executorSpec;
  private int registerRetryTimes;
  private int heartbeatIntervalMs;
  private int heartbeatRetryTimes;
  private InputManager inputManager;
  private List<String> portList;
  private String apiServerHost;
  private int apiServerPort;

  public PrimusExecutorConf(String amHost, int amPort, String role, int index, long uniqId,
      String apiServerHost, int apiServerPort, PrimusConf primusConf, String portRangesStr)
      throws PrimusExecutorException {
    this.amHost = amHost;
    this.amPort = amPort;
    executorId = new ExecutorIdPBImpl();
    executorId.setRoleName(role);
    executorId.setIndex(index);
    executorId.setUniqId(uniqId);

    this.primusConf = primusConf;

    this.portList = new ArrayList<>();
    if (portRangesStr != null) {
      String trimedStr = portRangesStr.replace("[", "").replace("]", "");
      this.portList = Arrays.asList(trimedStr.split(","));
    }

    this.apiServerHost = apiServerHost;
    this.apiServerPort = apiServerPort;

    if (!isLocalRunningMode()) {
      try {
        LOG.info("Create api client, current_executor:" + executorId.toUniqString());
        Client client = new DefaultClient(apiServerHost, apiServerPort);
        coreApi = new CoreApi(client);
        executorSpec = coreApi.getExecutor(executorId.toUniqString()).getSpec();
      } catch (Exception e) {
        throw new PrimusExecutorException("Failed to get executor from api server", e,
            ExecutorExitCode.GET_EXECUTOR_FAILED.getValue());
      }
    }
    inputManager = primusConf.getInputManager();
    registerRetryTimes = primusConf.getScheduler().getRegisterRetryTimes();
    heartbeatIntervalMs = primusConf.getScheduler().getHeartbeatIntervalMs();
    heartbeatRetryTimes = primusConf.getScheduler().getMaxMissedHeartbeat();
  }

  public String getAmHost() {
    return amHost;
  }

  public int getAmPort() {
    return amPort;
  }

  public ExecutorId getExecutorId() {
    return executorId;
  }

  public CoreApi getCoreApi() {
    return coreApi;
  }

  public ExecutorSpec getExecutorSpec() {
    return executorSpec;
  }

  public int getRegisterRetryTimes() {
    return registerRetryTimes;
  }

  public int getHeartbeatIntervalMs() {
    return heartbeatIntervalMs;
  }

  public int getHeartbeatRetryTimes() {
    return heartbeatRetryTimes;
  }

  public PrimusConf getPrimusConf() {
    return primusConf;
  }

  public InputManager getInputManager() {
    return inputManager;
  }

  public List<String> getPortList() {
    return portList;
  }

  public String getApiServerHost() {
    return apiServerHost;
  }

  public int getApiServerPort() {
    return apiServerPort;
  }

  public boolean isKubernetesRunningMode() {
    return primusConf.getRunningMode() == RunningMode.KUBERNETES;
  }

  public boolean isYarnRunningMode() {
    return primusConf.getRunningMode() == RunningMode.YARN;
  }

  public boolean isLocalRunningMode() {
    return primusConf.getRunningMode() == RunningMode.LOCAL;
  }
}
