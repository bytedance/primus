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

import com.bytedance.primus.api.protocolrecords.HeartbeatResponse;
import com.bytedance.primus.api.protocolrecords.RegisterRequest;
import com.bytedance.primus.api.protocolrecords.RegisterResponse;
import com.bytedance.primus.api.protocolrecords.UnregisterRequest;
import com.bytedance.primus.api.protocolrecords.impl.pb.HeartbeatRequestPBImpl;
import com.bytedance.primus.api.protocolrecords.impl.pb.HeartbeatResponsePBImpl;
import com.bytedance.primus.api.protocolrecords.impl.pb.RegisterRequestPBImpl;
import com.bytedance.primus.api.protocolrecords.impl.pb.RegisterResponsePBImpl;
import com.bytedance.primus.api.protocolrecords.impl.pb.UnregisterRequestPBImpl;
import com.bytedance.primus.api.records.ClusterSpec;
import com.bytedance.primus.api.records.Endpoint;
import com.bytedance.primus.api.records.ExecutorCommandType;
import com.bytedance.primus.api.records.ExecutorSpec;
import com.bytedance.primus.api.records.TaskCommand;
import com.bytedance.primus.api.records.impl.pb.EndpointPBImpl;
import com.bytedance.primus.api.records.impl.pb.ExecutorSpecPBImpl;
import com.bytedance.primus.common.event.Dispatcher;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.network.NetworkEndpointTypeEnum;
import com.bytedance.primus.common.service.AbstractService;
import com.bytedance.primus.executor.exception.PrimusExecutorException;
import com.bytedance.primus.executor.task.TaskAssignEvent;
import com.bytedance.primus.executor.task.TaskRemoveEvent;
import com.bytedance.primus.executor.worker.WorkerContext;
import com.bytedance.primus.proto.ExecutorTrackerServiceGrpc;
import com.bytedance.primus.proto.ExecutorTrackerServiceGrpc.ExecutorTrackerServiceBlockingStub;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorStatusUpdater extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorStatusUpdater.class);
  private static final Gson gson = new Gson();
  private static final int MAX_MESSAGE_SIZE = 1024 * 1024 * 128; // TODO: Centralize constants

  private final ExecutorContext executorContext;
  private final Dispatcher dispatcher;
  private volatile boolean isStopped;
  private final ExecutorStatusAPIServerUpdater executorStatusAPIServerUpdater;
  private boolean registered;

  // Communication between Primus Application Master
  private ManagedChannel amManagedChannel;
  private ExecutorTrackerServiceBlockingStub amBlockingStub;

  public ExecutorStatusUpdater(ExecutorContext executorContext, Dispatcher dispatcher) {
    super(ExecutorStatusUpdater.class.getName());
    this.executorContext = executorContext;
    this.dispatcher = dispatcher;
    isStopped = false;
    executorStatusAPIServerUpdater = new ExecutorStatusAPIServerUpdater(executorContext);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    PrimusExecutorConf primusExecutorConf = executorContext.getPrimusExecutorConf();
    amManagedChannel = ManagedChannelBuilder
        .forAddress(
            primusExecutorConf.getAmHost(),
            primusExecutorConf.getAmPort())
        .maxInboundMessageSize(MAX_MESSAGE_SIZE)
        .usePlaintext(true)
        .build();
    amBlockingStub = ExecutorTrackerServiceGrpc.newBlockingStub(amManagedChannel);

    LOG.info("Setting up ExecutorTrackerService: {}:{}",
        primusExecutorConf.getAmHost(),
        primusExecutorConf.getAmPort()
    );
  }

  @Override
  protected void serviceStart() {
    registerWithAm();
    startStatusUpdater();
    startExecutorStatusAPIServerUpdater();
  }


  private void registerWithAm() throws PrimusExecutorException {
    // Send register request
    ExecutorSpec executorSpec =
        (new ExecutorSpecPBImpl())
            .setExecutorId(executorContext.getExecutorId())
            .setEndpoints(collectAMEndpointsFromExecutorContext());

    RegisterRequest request = (new RegisterRequestPBImpl())
        .setExecutorId(executorContext.getExecutorId())
        .setExecutorSpec(executorSpec);

    // TODO: Refine Retry library with logger and terminating exception wrapping
    ClusterSpec clusterSpec = null;
    RegisterResponse response = null;
    for (int times = 0;
        clusterSpec == null && times < executorContext.getPrimusExecutorConf()
            .getRegisterRetryTimes();
        ++times) {
      LOG.info("executor " + executorContext.getExecutorId().toString() +
          " is registering, retryTimes " + times);

      try {
        response = new RegisterResponsePBImpl(amBlockingStub.register(request.getProto()));
        clusterSpec = response.getClusterSpec();
        Thread.sleep(executorContext.getPrimusExecutorConf().getHeartbeatIntervalMs());
      } catch (InterruptedException e) {
        // ignore
      } catch (Exception e) {
        LOG.warn("register fail:", e);
      }
    }

    // Finalizing registration
    if (response == null || response.getClusterSpec() == null) {
      throw new PrimusExecutorException("register with am failed",
          ExecutorExitCode.REGISTERED_FAIL.getValue());
    }

    LOG.info("executor " + executorContext.getExecutorId().toString() + " registered");
    registered = true;
    dispatcher.getEventHandler().handle(
        new ExecutorRegisteredEvent(
            new WorkerContext(
                response.getCommand().getCommand(),
                response.getCommand().getEnvironment(),
                response.getClusterSpec(),
                response.getCommand().getRestartTimes()),
            executorContext.getExecutorId()));
  }

  private List<Endpoint> collectAMEndpointsFromExecutorContext() {
    List<Endpoint> endpoints = new ArrayList<>();
    for (ServerSocket socket : executorContext.getFrameworkSocketList()) {
      EndpointPBImpl endpoint = new EndpointPBImpl();

      NetworkEndpointTypeEnum endpointType = executorContext
          .getNetworkConfig()
          .getNetworkEndpointType();
      endpoint.setHostname(endpointType == NetworkEndpointTypeEnum.IPADDRESS
          ? socket.getInetAddress().getHostAddress()
          : socket.getInetAddress().getHostName()
      );

      endpoint.setPort(socket.getLocalPort());
      endpoints.add(endpoint);
    }

    return endpoints;
  }

  private void startStatusUpdater() {
    Thread statusUpdater = new StatusUpdater();
    statusUpdater.start();
  }

  private void startExecutorStatusAPIServerUpdater() {
    ThreadFactory factoryBuilder = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("ExecutorStatusAPIServerUpdaterThread")
        .build();
    Executors.newSingleThreadExecutor(factoryBuilder)
        .execute(executorStatusAPIServerUpdater);
    LOG.info("ExecutorApiServerStatusUpdater started.");
  }

  @Override
  protected void serviceStop() throws Exception {
    // Interrupt the updater.
    this.isStopped = true;
    if (this.amManagedChannel != null) {
      if (!registered) {
        reportRegisterFailed();
      }
      amManagedChannel.shutdown();
    }
    super.serviceStop();
  }

  private void reportRegisterFailed() {
    try {
      LOG.error("Try to report executor status, because executor register with am failed");
      amBlockingStub.unregister(
          (new UnregisterRequestPBImpl())
              .setExecutorId(executorContext.getExecutorId())
              .setExitCode(ExecutorExitCode.REGISTERED_FAIL.getValue())
              .setFailMsg("register with am failed")
              .getProto());
    } catch (Exception e) {
      LOG.error("Failed to unregister", e);
    }
  }

  private void abort() {
    this.isStopped = true;
    dispatcher.getEventHandler().handle(new ContainerEvent(ContainerEventType.SHUTDOWN));
  }

  class StatusUpdater extends Thread {

    private int retryTimes = 0;
    private static final int UNREGISTER_MAX_RETRY_TIMES = 10;
    private static final int UNREGISTER_MAX_RETRY_INTERVAL_MS = 1000;

    @Override
    public void run() {
      while (!isStopped) {
        try {
          ExecutorStatus executorStatus = executorContext.getExecutor()
              .cloneAndGetExecutorStatus();
          heartbeat(executorStatus);
          switch (executorStatus.getExecutorState()) {
            case EXITED_WITH_SUCCESS:
            case EXITED_WITH_KILLED:
            case EXITED_WITH_FAILURE:
              if (!executorContext.getRunningEnvironment().hasExecutorJvmShutdown()) {
                LOG.info("Unregister...");
                unregister(executorStatus);
              } else {
                LOG.info("Killed By External Signal, Do Not Report Status To Am!");
              }
              abort();
              break;
            default:
              break;
          }
          try {
            Thread.sleep(executorContext.getPrimusExecutorConf().getHeartbeatIntervalMs());
          } catch (InterruptedException e) {
            // ignore
          }
        } catch (PrimusExecutorException e) {
          LOG.warn("status updater caught a exception", e);
          abort();
        }
      }
    }

    private void heartbeat(ExecutorStatus executorStatus) throws PrimusExecutorException {
      HeartbeatRequestPBImpl request = new HeartbeatRequestPBImpl();
      request.setExecutorId(executorContext.getExecutorId());
      request.setExecutorState(executorStatus.getExecutorState());
      request.setNeedMoreTask(executorStatus.getNeedMoreTask() && executorContext.isRunning());
      request.setTaskStatuses(executorContext.getTaskRunnerManager().getTaskStatusList());

      // TODO: Migrate to retry lib
      try {
        LOG.debug("Sending heartbeat: {}", gson.toJson(request));
        PrimusMetrics.TimerMetric heartbeatLatency =
            PrimusMetrics.getTimerContextWithAppIdTag(
                "executor.heartbeat.latency",
                new HashMap<String, String>() {{
                  put("executor_id", executorContext.getExecutorId().toString());
                }}
            );

        HeartbeatResponse response =
            new HeartbeatResponsePBImpl(amBlockingStub.heartbeat(request.getProto()));

        heartbeatLatency.stop();
        LOG.debug("Receiving heartbeat response: {}", gson.toJson(response));

        handleExecutorCommand(response.getExecutorCommandType());
        if (!response.isTaskReady()) {
          LOG.warn("Cannot get task manager, task manager is not ready");
        }

        handleTaskCommands(response.getTaskCommands());
        retryTimes = 0;
      } catch (Exception e) {
        LOG.warn("Heartbeat rpc error, retry times " + retryTimes, e);
        ++retryTimes;
      }

      if (retryTimes >= executorContext.getPrimusExecutorConf().getHeartbeatRetryTimes()) {
        throw new PrimusExecutorException("heartbeat failed",
            ExecutorExitCode.HEARTBEAT_FAIL.getValue());
      }
    }

    private void unregister(ExecutorStatus executorStatus) throws PrimusExecutorException {
      UnregisterRequest request =
          (new UnregisterRequestPBImpl())
              .setExecutorId(executorContext.getExecutorId())
              .setExitCode(executorStatus.getExitCode())
              .setFailMsg(executorStatus.getFailMsg());

      // TODO: Migrate to retry lib
      int retryTimes = 0;
      while (true) {
        try {
          amBlockingStub.unregister(request.getProto());
          try {
            executorStatusAPIServerUpdater.updateExecutorToApiServer(0,
                System.currentTimeMillis(),
                executorStatus.getExitCode(), executorStatus.getFailMsg());
          } catch (Exception e) {
            LOG.warn("Failed to update executor to api server", e);
          }
          break;
        } catch (Exception e) {
          LOG.warn("Unregister exception", e);
          ++retryTimes;
        }

        if (retryTimes >= UNREGISTER_MAX_RETRY_TIMES) {
          throw new PrimusExecutorException("heartbeat failed",
              ExecutorExitCode.UNREGISTER_FAIL.getValue());
        } else {
          try {
            Thread.sleep(UNREGISTER_MAX_RETRY_INTERVAL_MS);
          } catch (InterruptedException e) {
            // ignore
          }
        }
      }
    }

    private void handleExecutorCommand(ExecutorCommandType executorCommandType) {
      switch (executorCommandType) {
        case START:
          dispatcher.getEventHandler().handle(
              new ExecutorEvent(
                  ExecutorEventType.START,
                  executorContext.getExecutorId()));
          break;
        case KILL:
          dispatcher.getEventHandler().handle(
              new ExecutorEvent(
                  ExecutorEventType.KILL,
                  executorContext.getExecutorId()));
          break;
        default:
          break;
      }
    }

    private void handleTaskCommands(List<TaskCommand> taskCommands) {
      if (taskCommands.isEmpty()) {
        return;
      }

      for (TaskCommand taskCommand : taskCommands) {
        switch (taskCommand.getTaskCommandType()) {
          case ASSIGN:
            dispatcher.getEventHandler().handle(
                new TaskAssignEvent(taskCommand.getTask(), executorContext.getExecutorId()));
            break;
          case REMOVE:
            dispatcher.getEventHandler()
                .handle(new TaskRemoveEvent(taskCommand.getTask().getUid(),
                    executorContext.getExecutorId()));
            break;
          default:
            break;
        }
      }
    }
  }
}
