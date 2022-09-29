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

package com.bytedance.primus.runtime.yarncommunity.am;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ExecutorMonitor;
import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.runtime.yarncommunity.am.container.YarnContainerManager;
import com.bytedance.primus.runtime.yarncommunity.runtime.monitor.MonitorInfoProviderImpl;
import com.bytedance.primus.runtime.yarncommunity.utils.YarnConvertor;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnAMContext extends AMContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorMonitor.class);

  private AMRMClient<ContainerRequest> amRMClient;
  private NMClient nmClient;
  private Map<String, LocalResource> localResources;
  private String apiServerHost;
  private int apiServerPort;
  private YarnContainerManager containerManager;

  public YarnAMContext(PrimusConf primusConf) {
    super(primusConf);
    super.init(new MonitorInfoProviderImpl(this));
  }

  @Override
  // Let ExecutorMonitor periodically updates container status changes to API server.
  public boolean needToUpdateExecutorToApiServer() {
    return true;
  }

  @Override
  public Map<String, String> retrieveContainerMetric(Container container)
      throws IOException, PrimusRuntimeException {

    try {
      org.apache.hadoop.yarn.api.records.Container yarnContainer =
          YarnConvertor.toYarnContainer(container);

      LOGGER.info("Retrieving container status for (container: {}, yarn container: {})",
          container, yarnContainer);

      return YarnConvertor.toMetricToMap(
          getNmClient()
              .getContainerStatus(
                  yarnContainer.getId(),
                  yarnContainer.getNodeId()));

    } catch (YarnException e) {
      // TODO: remove this workaround
      return new HashMap<>(); // throw new PrimusRuntimeException(e);
    }
  }

  public AMRMClient<ContainerRequest> getAmRMClient() {
    return amRMClient;
  }

  public void setAmRMClient(AMRMClient<ContainerRequest> amRMClient) {
    this.amRMClient = amRMClient;
  }

  public NMClient getNmClient() {
    return nmClient;
  }

  public void setNmClient(NMClient nmClient) {
    this.nmClient = nmClient;
  }

  public Map<String, LocalResource> getLocalResources() {
    return localResources;
  }

  public void setLocalResources(
      Map<String, LocalResource> localResources) {
    this.localResources = localResources;
  }

  public String getApiServerHost() {
    return apiServerHost;
  }

  public void setApiServerHost(String apiServerHost) {
    this.apiServerHost = apiServerHost;
  }

  public int getApiServerPort() {
    return apiServerPort;
  }

  public void setApiServerPort(int apiServerPort) {
    this.apiServerPort = apiServerPort;
  }

  public YarnContainerManager getContainerManager() {
    return containerManager;
  }

  public void setContainerManager(
      YarnContainerManager containerManager) {
    this.containerManager = containerManager;
  }
}
