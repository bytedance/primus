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

import static com.bytedance.primus.utils.PrimusConstants.KILLED_THROUGH_AM_DIAG;

import com.bytedance.primus.am.controller.SuspendStatusEnum;
import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.am.datastream.file.FileTaskManager;
import com.bytedance.primus.apiserver.client.models.DataSavepoint;
import com.bytedance.primus.apiserver.proto.DataProto.DataSavepointStatus.DataSavepointState;
import com.bytedance.primus.apiserver.records.Meta;
import com.bytedance.primus.apiserver.records.impl.DataSavepointSpecImpl;
import com.bytedance.primus.apiserver.records.impl.MetaImpl;
import com.bytedance.primus.apiserver.service.exception.ApiServerException;
import com.bytedance.primus.proto.AmSerivce;
import com.bytedance.primus.proto.AmSerivce.CreateSavepointRequest;
import com.bytedance.primus.proto.AmSerivce.CreateSavepointResponse;
import com.bytedance.primus.proto.AmSerivce.CreateSavepointStatusRequest;
import com.bytedance.primus.proto.AmSerivce.CreateSavepointStatusResponse;
import com.bytedance.primus.proto.AmSerivce.CreateSavepointStatusResponse.CreateSavepointState;
import com.bytedance.primus.proto.AmSerivce.GetSnapshotRequest;
import com.bytedance.primus.proto.AmSerivce.GetSnapshotResponse;
import com.bytedance.primus.proto.AmSerivce.StatusRequest;
import com.bytedance.primus.proto.AmSerivce.StatusResponse;
import com.bytedance.primus.proto.AmSerivce.SuspendStatusRequest;
import com.bytedance.primus.proto.AmSerivce.SuspendStatusResponse;
import com.bytedance.primus.proto.AmSerivce.UpdateProgressRequest;
import com.bytedance.primus.proto.AmSerivce.UpdateProgressResponse;
import com.bytedance.primus.proto.AppMasterServiceGrpc;
import com.esotericsoftware.minlog.Log;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMServiceServer extends AppMasterServiceGrpc.AppMasterServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(AMServiceServer.class);

  private AMContext context;

  public AMServiceServer(AMContext context) {
    this.context = context;
  }

  @Override
  public void succeed(AmSerivce.SucceedRequest request,
      StreamObserver<AmSerivce.SucceedResponse> responseObserver) {
    String diag = request.getDiagnose();
    int exitCode = request.getExitCode() != 0 ? request.getExitCode()
        : ApplicationExitCode.SUCCESS_BY_RPC.getValue();
    if (request.hasGracefulShutdownTimeoutMs()) {
      long timeout = request.getGracefulShutdownTimeoutMs().getValue();
      context.emitApplicationSuccessEvent(diag, exitCode, timeout);
    } else {
      context.emitApplicationSuccessEvent(diag, exitCode);
    }
    AmSerivce.SucceedResponse.Builder builder = AmSerivce.SucceedResponse.newBuilder();
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void kill(AmSerivce.KillRequest request,
      StreamObserver<AmSerivce.KillResponse> responseObserver) {
    String diag = KILLED_THROUGH_AM_DIAG;
    if (!request.getDiagnose().equals("")) {
      diag = request.getDiagnose();
    }
    int exitCode = request.getExitCode() != 0 ? request.getExitCode()
        : ApplicationExitCode.KILLED_BY_RPC.getValue();

    if (request.hasGracefulShutdownTimeoutMs()) {
      long timeout = request.getGracefulShutdownTimeoutMs().getValue();
      context.emitFailAttemptEvent(diag, exitCode, timeout);
    } else {
      context.emitFailAttemptEvent(diag, exitCode);
    }

    AmSerivce.KillResponse.Builder builder = AmSerivce.KillResponse.newBuilder();
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void suspend(AmSerivce.SuspendRequest request,
      StreamObserver<AmSerivce.SuspendResponse> responseObserver) {
    String diag = "ApplicationMaster suspended through rpc request";
    LOG.info("sending ApplicationMasterEvent.SUSPEND_APP");
    context.emitSuspendApplicationEvent(diag, request.getSnapshotId());
    AmSerivce.SuspendResponse.Builder builder = AmSerivce.SuspendResponse.newBuilder();
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void suspendStatus(SuspendStatusRequest request,
      StreamObserver<SuspendStatusResponse> responseObserver) {
    SuspendStatusEnum suspendStatusEnum = context
        .getSuspendManager()
        .suspendStatus();
    boolean isSucceed = SuspendStatusEnum.FINISHED_SUCCESS == suspendStatusEnum;
    SuspendStatusResponse.Builder responseBuilder = SuspendStatusResponse.newBuilder()
        .setMessage("status: " + suspendStatusEnum.name())
        .setSucceed(isSucceed);
    responseObserver.onNext(responseBuilder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void resume(AmSerivce.ResumeRequest request,
      StreamObserver<AmSerivce.ResumeResponse> responseObserver) {
    String diag = "ApplicationMaster resume through rpc request";
    LOG.info("sending ApplicationMasterEvent.RESUME_APP");
    context.emitResumeApplicationEvent(diag);
    AmSerivce.ResumeResponse.Builder builder = AmSerivce.ResumeResponse.newBuilder();
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void getSnapshot(
      GetSnapshotRequest request,
      StreamObserver<GetSnapshotResponse> responseObserver
  ) {
    // TODO: Implement PrimusAPI
    responseObserver.onNext(AmSerivce
        .GetSnapshotResponse.newBuilder()
        .setAvailable(false)
        .build());
    responseObserver.onCompleted();
  }

  @Override
  public void progress(
      AmSerivce.ProgressRequest request,
      StreamObserver<AmSerivce.ProgressResponse> responseObserver
  ) {
    AmSerivce.ProgressResponse.Builder builder = AmSerivce.ProgressResponse.newBuilder();
    responseObserver
        .onNext(builder
            .setProgress(context.getProgressManager().getProgress())
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void isStarving(AmSerivce.StarvingRequest request,
      StreamObserver<AmSerivce.StarvingResponse> responseObserver) {
    /*
    AmSerivce.StarvingResponse.Builder builder = AmSerivce.StarvingResponse.newBuilder();
    responseObserver.onNext(builder.setStarving(context.getSchedulerExecutorManager().isStarving()).build());
     */
    responseObserver.onCompleted();
  }

  @Override
  public void status(
      StatusRequest request,
      StreamObserver<StatusResponse> responseObserver
  ) {
    String appId = context.getApplicationMeta().getApplicationId();
    String finalStatus = (context.getFinalStatus() != null)
        ? context.getFinalStatus().toString()
        : "IN_PROGRESS";

    String trackUrl = "http://"
        + context.getWebAppServerHostAddress() + ":"
        + context.getWebAppServerPort() + "/webapps/primus/";

    responseObserver.onNext(
        StatusResponse.newBuilder()
            .setAppId(appId)
            .setFinalStatus(finalStatus)
            .setTrackUrl(trackUrl)
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void getTaskTimePoint(AmSerivce.TaskTimePointRequest request,
      StreamObserver<AmSerivce.TaskTimePointResponse> responseObserver) {
    AmSerivce.TaskTimePointResponse.Builder builder = AmSerivce.TaskTimePointResponse.newBuilder();
    // TODO: fixme, get the right task manager
    FileTaskManager fileTaskManager = context
        .getDataStreamManager()
        .getDefaultFileTaskManager();

    String batchKey = fileTaskManager
        .getPendingTasks(1).stream()
        .findFirst()
        .map(TaskWrapper::getTask)
        .map(task -> {
          switch (task.getTaskType()) {
            case FILE_TASK:
              return task.getFileTask().getBatchKey();
            default:
              return null;
          }
        })
        .orElse(null);

    responseObserver.onNext(builder.setTimePoint(batchKey).build());
    responseObserver.onCompleted();
  }

  @Override
  public void updateProgress(UpdateProgressRequest request,
      StreamObserver<UpdateProgressResponse> responseObserver) {
    float currentProgress = context.getProgressManager().getProgress();
    float requestedProgress = request.getProgress();
    LOG.info("Received update progress from:{}, to:{}", currentProgress, requestedProgress);
    try {
      context
          .getProgressManager()
          .setProgress(
              request.getAllowRewind(),
              requestedProgress
          );
      float updatedProgress = context.getProgressManager().getProgress();
      String message = String
          .format("success update progress from: %f, to: %f", currentProgress, updatedProgress);
      responseObserver.onNext(UpdateProgressResponse.newBuilder().setMessage(message).build());
    } catch (Exception ex) {
      LOG.error("Error when update progress through grpc", ex);
      String message = "Error when update progress:" + ex.getMessage();
      responseObserver.onNext(
          UpdateProgressResponse.newBuilder().setMessage(message).setCode(-1).build());
    } finally {
      responseObserver.onCompleted();
    }
  }

  @Override
  public void createSavepoint(CreateSavepointRequest request,
      StreamObserver<CreateSavepointResponse> responseObserver) {
    Preconditions.checkState(!Strings.isNullOrEmpty(request.getSavepointDir()));
    DataSavepoint dataSavepoint = new DataSavepoint();
    String dateTimeStr = DateTime.now().toString("yyyy-MM-dd-HH-mm-ss");
    String uuidStr = UUID.randomUUID().toString();
    String name = dateTimeStr + "_" + uuidStr;
    LOG.info("Received create Savepoint request:{}, savepoint id:{}", request, name);
    Meta meta = new MetaImpl().setName(name);
    DataSavepointSpecImpl spec = new DataSavepointSpecImpl();
    spec.setSavepointDir(request.getSavepointDir());

    dataSavepoint.setMeta(meta);
    dataSavepoint.setSpec(spec);
    try {
      DataSavepoint createdDataSavepoint = context
          .getCoreApi()
          .createDataSavepoint(dataSavepoint);
      CreateSavepointResponse response = CreateSavepointResponse.newBuilder()
          .setCode(0)
          .setMessage("successfully create savepoint:" + createdDataSavepoint.getMeta().getName())
          .setSavepointId(name)
          .build();
      responseObserver.onNext(response);
    } catch (ApiServerException e) {
      Log.error("error when create savepoint", e);
      CreateSavepointResponse response = CreateSavepointResponse.newBuilder()
          .setCode(-1)
          .setMessage("error, message" + e.getMessage())
          .build();
      responseObserver.onNext(response);
    }
    responseObserver.onCompleted();
  }

  @Override
  public void createSavepointStatus(CreateSavepointStatusRequest request,
      StreamObserver<CreateSavepointStatusResponse> responseObserver) {
    Preconditions.checkState(!Strings.isNullOrEmpty(request.getSavepointRestoreId()));
    try {
      DataSavepoint dataSavepoint = context
          .getCoreApi()
          .getDataSavepoint(request.getSavepointRestoreId());
      DataSavepointState state = dataSavepoint.getStatus().getState();
      LOG.info("Current data savepoint id:{}, state:{}", request.getSavepointRestoreId(), state);
      CreateSavepointStatusResponse response = CreateSavepointStatusResponse.newBuilder()
          .setCode(0)
          .setMessage("success")
          .setCreateSavepointState(getSavepointStatus(state))
          .build();
      responseObserver.onNext(response);
    } catch (ApiServerException e) {
      Log.error("error when get savepoint status", e);
      CreateSavepointStatusResponse response = CreateSavepointStatusResponse.newBuilder()
          .setCode(-1)
          .setMessage("error, message:" + e.getMessage())
          .build();
      responseObserver.onNext(response);
    }
    responseObserver.onCompleted();
  }

  private CreateSavepointState getSavepointStatus(DataSavepointState savepointState) {
    return CreateSavepointState.forNumber(savepointState.getNumber());
  }
}
