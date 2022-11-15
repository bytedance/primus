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

package com.bytedance.primus.am.eventlog;

import static com.bytedance.primus.webapp.StatusServlet.buildTaskUri;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.datastream.TaskManager;
import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.am.datastream.file.FileTaskManager;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorImpl;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.common.model.records.FinalApplicationStatus;
import com.bytedance.primus.proto.EventLog.MsgDataPrimusJobStateChange;
import com.bytedance.primus.proto.EventLog.MsgDataPrimusJobStateChange.AppState;
import com.bytedance.primus.proto.EventLog.MsgDataTaskStateChange;
import com.bytedance.primus.proto.EventLog.MsgDataTaskStateChange.TaskState;
import com.bytedance.primus.proto.EventLog.MsgDataWorkerStateChange;
import com.bytedance.primus.proto.EventLog.MsgDataWorkerStateChange.WorkerState;
import com.bytedance.primus.proto.EventLog.PrimusEventMsg;
import com.bytedance.primus.utils.ProtoJsonConverter;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusEventWrapper {

  private static final Logger log = LoggerFactory.getLogger(StatusEventWrapper.class);
  private AMContext context;

  public StatusEventWrapper(AMContext context) {
    this.context = context;
  }

  public AMStatusEvent buildAMStartEvent() {
    PrimusEventMsg.Builder msg = PrimusEventMsg.newBuilder();
    try {
      if (context.getStatusLoggingListener().canLogEvent()) {
        msg.setYarnApplicationId(context.getApplicationId());
        MsgDataPrimusJobStateChange.Builder builder = MsgDataPrimusJobStateChange.newBuilder();
        builder.setName(context.getPrimusConf().getName());
        builder.setTimeStamp(context.getStartTime().getTime());
        builder.setProgress(context.getProgressManager().getProgress());
        builder.setStateValue(AppState.NEW_VALUE);
        builder.setPrimusConf(ProtoJsonConverter.getJsonString(context.getPrimusConf()));
        msg.setJobStateData(builder.build());
      }
    } catch (Exception e) {
      log.error("Failed to build AmStartEvent", e);
    }

    AMStatusEvent amStartEvent = new AMStatusEvent(msg.build());
    return amStartEvent;
  }

  public AMStatusEvent buildAMEndEvent(
      FinalApplicationStatus status, int exitCode, String diagnosis) {
    PrimusEventMsg.Builder msg = PrimusEventMsg.newBuilder();
    try {
      if (context.getStatusLoggingListener().canLogEvent()) {
        msg.setYarnApplicationId(context.getApplicationId());
        MsgDataPrimusJobStateChange.Builder builder = MsgDataPrimusJobStateChange.newBuilder();
        builder.setName(context.getPrimusConf().getName());
        builder.setTimeStamp(System.currentTimeMillis());
        builder.setProgress(context.getProgressManager().getProgress());
        switch (status) {
          case SUCCEEDED:
            builder.setStateValue(AppState.SUCCEEDED_VALUE);
            break;
          case FAILED:
            builder.setStateValue(AppState.FAILED_VALUE);
            break;
          case KILLED:
            builder.setStateValue(AppState.KILLED_VALUE);
            break;
          default:
        }
        builder.setStateValue(AppState.SUCCEEDED_VALUE);
        builder.setExitCode(exitCode);
        builder.setDiagnosis(diagnosis);
        msg.setJobStateData(builder.build());
      }
    } catch (Exception e) {
      log.error("Failed to build AmEndEvent", e);
    }
    AMStatusEvent amEndEvent = new AMStatusEvent(msg.build());
    return amEndEvent;
  }

  public WorkerStatusEvent buildWorkerStatusEvent(SchedulerExecutorImpl schedulerExecutor) {
    PrimusEventMsg.Builder msg = PrimusEventMsg.newBuilder();
    try {
      if (context.getStatusLoggingListener() != null &&
          context.getStatusLoggingListener().canLogEvent()) {
        msg.setYarnApplicationId(context.getApplicationId());
        MsgDataWorkerStateChange.Builder builder = MsgDataWorkerStateChange.newBuilder();
        ExecutorId executorId = schedulerExecutor.getExecutorId();
        builder.setId(executorId.getIndex());
        builder.setRoleName(executorId.getRoleName());
        builder.setWorkerName(executorId.toUniqString());
        builder.setTimeStamp(System.currentTimeMillis());
        builder.setExitCode(schedulerExecutor.getExecutorExitCode());
        builder.setDiagnosis(schedulerExecutor.getContainerExitMsg());
        builder.setState(WorkerState.valueOf(schedulerExecutor.getExecutorState().name()));
        msg.setWorkerStateData(builder.build());
      }
    } catch (Exception e) {
      log.error("Failed to build WorkerStatusEvent", e);
    }
    WorkerStatusEvent workerStatusEvent = new WorkerStatusEvent(msg.build());
    return workerStatusEvent;
  }

  public List<TaskStatusEvent> buildAllTaskStatusEvent() {
    List<TaskStatusEvent> res = Lists.newArrayList();
    try {
      if (context.getStatusLoggingListener().canLogEvent()) {
        for (Entry<String, TaskManager> taskManagerEntry :
            context.getDataStreamManager().getTaskManagerMap().entrySet()) {
          TaskManager taskManager = taskManagerEntry.getValue();
          if (taskManager instanceof FileTaskManager) {
            FileTaskManager fileTaskManager = (FileTaskManager) taskManager;
            List<TaskWrapper> tasks = fileTaskManager.getTasksForHistory();
            for (TaskWrapper taskWrapper : tasks) {
              PrimusEventMsg.Builder msg = PrimusEventMsg.newBuilder();
              msg.setYarnApplicationId(context.getApplicationId());
              MsgDataTaskStateChange.Builder taskState = MsgDataTaskStateChange.newBuilder();
              Task task = taskWrapper.getTask();
              TaskStatus status = taskWrapper.getTaskStatus();
              taskState.setState(TaskState.valueOf(status.getTaskState().name()));
              if (status.getNumAttempt() == 0) {
                taskState.setState(TaskState.PENDING);
              }
              taskState.setId(task.getTaskId());
              taskState.setProgress(status.getProgress());
              taskState.setAttempt(status.getNumAttempt());
              taskState.setFinishTime(status.getFinishTime());
              taskState.setLastAssignTime(status.getLastAssignTime());
              taskState.setURI(buildTaskUri(task));
              taskState.setWorkerName(status.getWorkerName());
              msg.setTaskStateData(taskState.build());
              res.add(new TaskStatusEvent(msg.build()));
            }
          }
        }
      }
    } catch (Exception e) {
      log.error("Failed to build allTaskStatusEvent", e);
    }

    return res;
  }

  public TaskStatusEvent buildTaskStatusEvent(TaskWrapper taskWrapper) {
    PrimusEventMsg.Builder msg = PrimusEventMsg.newBuilder();
    try {
      if (context.getStatusLoggingListener().canLogEvent()) {
        msg.setYarnApplicationId(context.getApplicationId());
        MsgDataTaskStateChange.Builder taskState = MsgDataTaskStateChange.newBuilder();
        Task task = taskWrapper.getTask();
        TaskStatus status = taskWrapper.getTaskStatus();
        taskState.setState(TaskState.valueOf(status.getTaskState().name()));
        if (status.getNumAttempt() == 0) {
          taskState.setState(TaskState.PENDING);
        }
        taskState.setId(task.getTaskId());
        taskState.setProgress(status.getProgress());
        taskState.setAttempt(status.getNumAttempt());
        taskState.setFinishTime(status.getFinishTime());
        taskState.setLastAssignTime(status.getLastAssignTime());
        taskState.setURI(buildTaskUri(task));
        taskState.setWorkerName(status.getWorkerName());
        msg.setTaskStateData(taskState.build());
      }
    } catch (Exception e) {
      log.error("Failed to build taskStatusEvent", e);
    }
    TaskStatusEvent taskStatusEvent = new TaskStatusEvent(msg.build());
    return taskStatusEvent;
  }
}
