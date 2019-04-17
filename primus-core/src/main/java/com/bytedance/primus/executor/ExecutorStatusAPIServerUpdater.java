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

import static com.bytedance.primus.utils.PrimusConstants.UPDATE_TO_API_SERVER_RETRY_INTERVAL_MS;
import static com.bytedance.primus.utils.PrimusConstants.UPDATE_TO_API_SERVER_RETRY_TIMES;

import com.bytedance.primus.apiserver.client.apis.CoreApi;
import com.bytedance.primus.apiserver.client.models.Executor;
import com.bytedance.primus.apiserver.service.exception.ApiServerException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorStatusAPIServerUpdater implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(ExecutorStatusAPIServerUpdater.class);

  private CoreApi coreApi;
  private ExecutorContext executorContext;

  public ExecutorStatusAPIServerUpdater(ExecutorContext executorContext) {
    this.coreApi = executorContext.getPrimusConf().getCoreApi();
    this.executorContext = executorContext;
  }

  @Override
  public void run() {
    while (true) {
      try {
        updateExecutorToApiServer(0, 0, 0, null);
        try {
          Thread.sleep(executorContext.getPrimusConf().getHeartbeatIntervalMs());
        } catch (InterruptedException e) {
        }
      } catch (Exception e) {
        log.warn("Failed to update executor to api server", e);
      }
    }

  }

  public void updateExecutorToApiServer(long startTime, long completionTime,
      int exitStatus, String diagnostics)
      throws ApiServerException {
    int retryTimes = 0;
    while (true) {
      try {
        Executor executor = coreApi.getExecutor(executorContext.getExecutorId().toUniqString());
        if (startTime > 0) {
          executor.getStatus().setStartTime(System.currentTimeMillis());
        }
        if (completionTime > 0) {
          executor.getStatus().setCompleteTime(completionTime);
        }
        // TODO: Check if we should set exit status
        executor.getStatus().setExitStatus(exitStatus);
        if (diagnostics != null) {
          executor.getStatus().setDiagnostics(diagnostics);
        }
        if (!executor.getStatus().getNetworkSockets().equals(getNetworkSockets())) {
          executor.getStatus().setNetworkSockets(getNetworkSockets());
        }
        if (!executor.getStatus().getHostname().equals(executorContext.getHostname())) {
          executor.getStatus().setHostname(executorContext.getHostname());
        }
        coreApi.replaceExecutor(executor, executor.getMeta().getVersion());
        break;
      } catch (ApiServerException e) {
        retryTimes++;
        if (retryTimes >= UPDATE_TO_API_SERVER_RETRY_TIMES) {
          throw e;
        }
      }
      try {
        Thread.sleep(UPDATE_TO_API_SERVER_RETRY_INTERVAL_MS);
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }

  private List<String> getNetworkSockets() {
    return executorContext.getFrameworkSocketList().stream()
        .map(s -> {
          InetSocketAddress localSocketAddress = (InetSocketAddress) s.getLocalSocketAddress();
          return localSocketAddress.getAddress().getHostAddress() + ":" + localSocketAddress
              .getPort();
        }).collect(Collectors.toList());
  }
}
