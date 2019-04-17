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

package com.bytedance.primus.runtime.kubernetesnative.common.operator.status.service.impl;

import com.bytedance.primus.apiserver.client.Client;
import com.bytedance.primus.apiserver.client.DefaultClient;
import com.bytedance.primus.apiserver.client.apis.CoreApi;
import com.bytedance.primus.apiserver.client.models.Executor;
import com.bytedance.primus.apiserver.client.models.Job;
import com.bytedance.primus.apiserver.records.JobStatus;
import com.bytedance.primus.apiserver.service.exception.ApiServerException;
import com.bytedance.primus.runtime.kubernetesnative.ResourceNameBuilder;
import com.bytedance.primus.runtime.kubernetesnative.common.operator.status.model.APIServerEndPoint;
import com.bytedance.primus.runtime.kubernetesnative.common.operator.status.model.OperatorJobStatus;
import com.bytedance.primus.runtime.kubernetesnative.common.operator.status.service.JobStatusManagerService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobStatusManagerServiceImpl implements JobStatusManagerService {

  public static final String DEFAULT_JOB_STATUS_UNDEFINED = "UNDEFINED";
  public static final String DEFAULT_AM_STATUS_UNDEFINED = "UNDEFINED";
  private static Logger LOGGER = LoggerFactory.getLogger(JobStatusManagerServiceImpl.class);
  private APIServerEndPoint endPoint;
  private String appName;
  private CoreApi coreApi;

  public JobStatusManagerServiceImpl(APIServerEndPoint endPoint, String appName) {
    this.endPoint = endPoint;
    this.appName = appName;
  }

  private synchronized void startCoreApiClient() throws Exception {
    if (coreApi != null) {
      return;
    }
    LOGGER.info("Create api client");
    Client client = new DefaultClient(endPoint.getHost(), endPoint.getPort());
    coreApi = new CoreApi(client);
  }

  @Override
  public OperatorJobStatus fetchLatest() throws Exception {
    if (coreApi == null) {
      startCoreApiClient();
    }
    OperatorJobStatus operatorJobStatus = new OperatorJobStatus();
    operatorJobStatus.setSubmissionTime(System.currentTimeMillis());
    operatorJobStatus.setLastUpdateTime(System.currentTimeMillis());

    List<Job> jobs = coreApi.listJobs();
    if (jobs.size() > 0) {
      Job job = jobs.stream().findFirst().get();
      JobStatus status = job.getStatus();
      operatorJobStatus.setJobState(DEFAULT_JOB_STATUS_UNDEFINED);
      operatorJobStatus.setSubmissionTime(status.getStartTime());
      operatorJobStatus.setLastUpdateTime(System.currentTimeMillis());
    }

    Map<String, String> driverStateMap = getAMState();
    operatorJobStatus.setAmState(driverStateMap);

    Map<String, String> executorStateMap = getExecutorState();
    operatorJobStatus.setExecutorState(executorStateMap);
    return operatorJobStatus;
  }

  private Map<String, String> getExecutorState() throws ApiServerException {
    Map<String, String> executorStateMap = new HashMap<>();
    List<Executor> executors = coreApi.listExecutors();
    for (Executor executor : executors) {
      String executorName = ResourceNameBuilder.buildExecutorPodName(appName,
          executor.getMeta().getName());
      executorStateMap.put(executorName, executor.getStatus().getState());
    }
    return executorStateMap;
  }

  private Map<String, String> getAMState() {
    String driverName = ResourceNameBuilder.buildDriverPodName(appName);
    Map<String, String> driverStateMap = new HashMap<>();
    driverStateMap.put(driverName, DEFAULT_AM_STATUS_UNDEFINED);
    return driverStateMap;
  }
}
