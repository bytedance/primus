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

package com.bytedance.primus.am;

import com.bytedance.primus.am.datastream.TaskManager;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManager;
import com.bytedance.primus.api.protocolrecords.impl.pb.HeartbeatRequestPBImpl;
import com.bytedance.primus.api.protocolrecords.impl.pb.HeartbeatResponsePBImpl;
import com.bytedance.primus.api.protocolrecords.impl.pb.RegisterRequestPBImpl;
import com.bytedance.primus.api.protocolrecords.impl.pb.RegisterResponsePBImpl;
import com.bytedance.primus.api.protocolrecords.impl.pb.UnregisterRequestPBImpl;
import com.bytedance.primus.api.protocolrecords.impl.pb.UnregisterResponsePBImpl;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.TaskCommand;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.proto.ExecutorTrackerServiceGrpc;
import com.bytedance.primus.proto.Primus.HeartbeatRequestProto;
import com.bytedance.primus.proto.Primus.HeartbeatResponseProto;
import com.bytedance.primus.proto.Primus.RegisterRequestProto;
import com.bytedance.primus.proto.Primus.RegisterResponseProto;
import com.bytedance.primus.proto.Primus.UnregisterRequestProto;
import com.bytedance.primus.proto.Primus.UnregisterResponseProto;
import com.google.gson.Gson;
import io.grpc.stub.StreamObserver;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorTrackerGrpcService extends
    ExecutorTrackerServiceGrpc.ExecutorTrackerServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorTrackerGrpcService.class);
  private static final Gson gson = new Gson();

  private final AMContext context;
  private final SchedulerExecutorManager schedulerExecutorManager;
  private final RoleInfoManager roleInfoManager;

  public ExecutorTrackerGrpcService(AMContext context) {
    this.context = context;
    this.schedulerExecutorManager = context.getSchedulerExecutorManager();
    this.roleInfoManager = context.getRoleInfoManager();
  }

  @Override
  public void register(
      RegisterRequestProto request,
      StreamObserver<RegisterResponseProto> responseObserver
  ) {
    RegisterRequestPBImpl requestPB = new RegisterRequestPBImpl(request);
    RegisterResponsePBImpl responsePB =
        (RegisterResponsePBImpl) schedulerExecutorManager.register(requestPB);

    responseObserver.onNext(responsePB.getProto());
    responseObserver.onCompleted();
  }

  @Override
  public void heartbeat(
      HeartbeatRequestProto request,
      StreamObserver<HeartbeatResponseProto> responseObserver
  ) {
    LOG.debug("Receiving heartbeat: {}", gson.toJson(request));

    HeartbeatRequestPBImpl requestPB = new HeartbeatRequestPBImpl(request);
    HeartbeatResponsePBImpl responsePB = new HeartbeatResponsePBImpl();

    responsePB.setExecutorCommandType(
        schedulerExecutorManager.heartbeat(
            requestPB.getExecutorId(),
            requestPB.getExecutorState()));

    responsePB.setTaskCommands(
        retrieveTaskCommands(
            requestPB.getExecutorId(),
            requestPB.getTaskStatuses(),
            requestPB.getNeedMoreTask()));

    // TODO: set to false if task manager is not ready in addition to its existence.
    responsePB.setTaskReady(context.getDataStreamManager() != null);

    LOG.debug("Responding heartbeat: {}", gson.toJson(request));
    responseObserver.onNext(responsePB.getProto());
    responseObserver.onCompleted();
  }

  @Override
  public void unregister(
      UnregisterRequestProto request,
      StreamObserver<UnregisterResponseProto> responseObserver
  ) {
    UnregisterRequestPBImpl requestPB = new UnregisterRequestPBImpl(request);
    UnregisterResponsePBImpl responsePB = new UnregisterResponsePBImpl();

    schedulerExecutorManager.unregister(
        requestPB.getExecutorId(),
        requestPB.getExitCode(),
        requestPB.getFailMsg());

    responseObserver.onNext(responsePB.getProto());
    responseObserver.onCompleted();
  }

  private List<TaskCommand> retrieveTaskCommands(
      ExecutorId executorId,
      List<TaskStatus> taskStatuses,
      boolean needMoreTask
  ) {
    // Preprocess
    String targetDataStream = roleInfoManager.getTaskManagerName(executorId);
    Map<String, List<TaskStatus>> taskStatusMap =
        taskStatuses.
            stream().
            collect(Collectors.groupingBy(TaskStatus::getGroup));

    // Collect TaskCommands for mismatched DataStreams
    List<TaskCommand> removeTaskCommands = taskStatusMap
        .entrySet()
        .stream()
        .filter(entry -> !targetDataStream.equals(entry.getKey()))
        .map(entry -> {
          // Fetch TaskCommands for the target DataStream and gracefully terminate the others
          return retrieveTaskCommandsForDataStream(
              executorId,
              entry.getKey(),
              entry.getValue(),
              false /* NeedMoreTask */,
              true /* RemoveTask */
          );
        })
        .reduce(new LinkedList<>(), (acc, taskCommands) -> {
          acc.addAll(taskCommands);
          return acc;
        });

    // Collect TaskCommands for the matching DataStream
    List<TaskCommand> upsertTaskCommands = retrieveTaskCommandsForDataStream(
        executorId,
        targetDataStream,
        taskStatusMap.getOrDefault(targetDataStream, new LinkedList<>()),
        needMoreTask,
        false /* RemoveTask */
    );

    // Assembly and return
    return new LinkedList<TaskCommand>() {{
      addAll(removeTaskCommands);
      addAll(upsertTaskCommands);
    }};
  }

  private List<TaskCommand> retrieveTaskCommandsForDataStream(
      ExecutorId executorId,
      String dataStream,
      List<TaskStatus> taskStatuses,
      boolean needMoreTask,
      boolean removeTask
  ) {
    TaskManager taskManager = context
        .getDataStreamManager()
        .getTaskManager(dataStream);

    if (taskManager == null) {
      LOG.warn(String.format(
          "Cannot get task manager for %s, task manager is not ready",
          executorId.toString()));
      return new LinkedList<>();
    }

    List<TaskCommand> taskCommands = taskManager.heartbeat(
        executorId, taskStatuses, removeTask, needMoreTask);

    if (!taskCommands.isEmpty()) {
      LOG.info("New TaskCommands for (Executor[{}], DataStream[{}]): {}",
          executorId, dataStream, gson.toJson(taskCommands));
    }

    return taskCommands;
  }
}
