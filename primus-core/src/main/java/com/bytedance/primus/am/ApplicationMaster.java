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

import static com.bytedance.primus.am.ApplicationExitCode.GANG_POLICY_FAILED;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_APPLICATION_MASTER_EVENT;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_APP_SHUTDOWN;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_VERSION;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_AM_RPC_HOST;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_AM_RPC_PORT;

import com.bytedance.primus.am.master.MasterContext;
import com.bytedance.primus.apiserver.client.models.Data;
import com.bytedance.primus.apiserver.client.models.Job;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.apiserver.records.RoleStatus;
import com.bytedance.primus.apiserver.utils.Constants;
import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.model.records.FinalApplicationStatus;
import com.bytedance.primus.common.service.CompositeService;
import com.bytedance.primus.common.util.Sleeper;
import com.bytedance.primus.proto.PrimusConfOuterClass.OrderSchedulePolicy.RolePolicy;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusInput.InputManager.ConfigCase;
import com.bytedance.primus.utils.AMProcessExitCodeHelper;
import com.bytedance.primus.utils.ResourceUtils;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ApplicationMaster is the center of a Primus application, which is responsible to tracking the
 * status of the Primus application and interacting with the runtime.
 */
public abstract class ApplicationMaster
    extends CompositeService
    implements EventHandler<ApplicationMasterEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMaster.class);

  protected final AMContext context;
  protected final PrimusConf primusConf;

  // States
  protected volatile boolean isStopped = false;
  protected volatile boolean gracefulShutdown = false;
  protected FinalApplicationStatus finalStatus = FinalApplicationStatus.UNDEFINED;
  protected int exitCode = ApplicationExitCode.UNDEFINED.getValue();

  protected boolean needUnregister = false;
  protected String unregisterDiagnostics = "NA";

  public ApplicationMaster(String name, AMContext context) {
    super(name);
    this.context = context;
    this.primusConf = context.getApplicationMeta().getPrimusConf();
  }

  /**
   * waitForStop() is a utility that helps caller harness the exit code after the completion of the
   * Primus application
   */
  public int waitForStop() {
    // For compatibility
    checkAndUpdateDataStream();

    while (!isStopped) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    abort();
    int exitCode = new AMProcessExitCodeHelper(context.getFinalStatus()).getExitCode();
    return exitCode;
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();

    context.logStatusEvent(context.getStatusEventWrapper().buildAMStartEvent());
    Thread.sleep(15 * 1000); // Watch interface is async so sleep a while to wait for creation

    if (primusConf.getScheduler().getCommand().isEmpty()) {
      Job job = ResourceUtils.buildJob(primusConf);
      if (primusConf.getScheduler().getSchedulePolicy().hasOrderPolicy()) {
        LOG.info("Add data to api server before schedule role by order");
        Data data = ResourceUtils.buildData(primusConf);
        context.getCoreApi().create(Data.class, data);
        List<String> roleNames = getRoleNames();
        scheduleRoleByOrder(job, roleNames);
      } else {
        context.getCoreApi().createJob(job);
      }
      return;
    }

    LOG.info("Start master");
    Map<String, String> envs = new HashMap<String, String>() {{
      putAll(System.getenv());
      put(Constants.API_SERVER_RPC_HOST_ENV, context.getApiServerHostAddress());
      put(Constants.API_SERVER_RPC_PORT_ENV, String.valueOf(context.getApiServerPort()));
      put(PRIMUS_AM_RPC_HOST, context.getAmService().getHostName());
      put(PRIMUS_AM_RPC_PORT, String.valueOf(context.getAmService().getPort()));
    }};

    context
        .emitChiefTrainerStartEvent(new MasterContext(
            primusConf.getScheduler().getCommand(),
            envs
        ));
  }

  @Override
  protected void serviceStop() throws Exception {
    isStopped = true;
    super.serviceStop();
  }

  protected abstract void abort();

  @Override
  public synchronized void handle(ApplicationMasterEvent event) {
    context.logTimelineEvent(PRIMUS_VERSION.name(), context.getApplicationMeta().getVersion());
    context.logTimelineEvent(PRIMUS_APPLICATION_MASTER_EVENT.name(), event.getType().name());
    switch (event.getType()) {
      case SUCCESS:
        if (!gracefulShutdown) {
          this.exitCode = event.getExitCode();
          LOG.info("Handle success app event, primus exit code: " + exitCode);
          if (!event.getDiagnosis().equals("")) {
            LOG.info("App success message: " + event.getDiagnosis());
          }
          finalStatus = FinalApplicationStatus.SUCCEEDED;
          needUnregister = true;
          unregisterDiagnostics = event.getDiagnosis();
          context.getProgressManager().setProgress(1f);
          gracefulShutdown(exitCode, event.getGracefulShutdownTimeoutMs());
        }
        break;
      case FAIL_ATTEMPT:
        if (!gracefulShutdown) {
          this.exitCode = event.getExitCode();
          LOG.error("Handle fail attempt event, cause: " + event.getDiagnosis()
              + ", primus exit code: " + exitCode);
          finalStatus = FinalApplicationStatus.FAILED;

          PrimusApplicationMeta meta = context.getApplicationMeta();
          needUnregister = meta.getAttemptId() >= meta.getMaxAttemptId();
          unregisterDiagnostics = event.getDiagnosis();
          gracefulShutdown(exitCode, event.getGracefulShutdownTimeoutMs());
        }
        break;
      case FAIL_APP:
        if (!gracefulShutdown) {
          this.exitCode = event.getExitCode();
          LOG.error("Handle fail app event, cause: " + event.getDiagnosis()
              + ", primus exit code: " + exitCode);
          finalStatus = FinalApplicationStatus.FAILED;
          needUnregister = true;
          unregisterDiagnostics = event.getDiagnosis();
          gracefulShutdown(exitCode, event.getGracefulShutdownTimeoutMs());
        }
        break;
      case SUSPEND_APP:
        ApplicationMasterSuspendAppEvent e = (ApplicationMasterSuspendAppEvent) event;
        LOG.info("Handle suspend app event, cause: " + event.getDiagnosis());
        context.getSuspendManager().suspend(e.getSnapshotId());
        break;
      case RESUME_APP:
        LOG.info("Handle resume app event, cause: " + event.getDiagnosis());
        context.getSuspendManager().resume();
        break;
      default:
        if (!gracefulShutdown) {
          this.exitCode = ApplicationExitCode.UNDEFINED.getValue();
          LOG.info("Unknown app event, primus exit code: " + exitCode);
          finalStatus = FinalApplicationStatus.UNDEFINED;

          PrimusApplicationMeta meta = context.getApplicationMeta();
          needUnregister = meta.getAttemptId() >= meta.getMaxAttemptId();
          unregisterDiagnostics = event.getDiagnosis();
          gracefulShutdown(exitCode, event.getGracefulShutdownTimeoutMs());
        }
        break;
    }
    if (gracefulShutdown) {
      context.logStatusEvent(context.getStatusEventWrapper()
          .buildAMEndEvent(finalStatus, exitCode, unregisterDiagnostics));
    }
  }

  public void gracefulShutdown(int exitCode, long gracefulShutdownTimeoutMs) {
    LOG.info("start gracefulShutdown, timeout:{}", gracefulShutdownTimeoutMs);
    PrimusMetrics.TimerMetric gracefulShutdownLatency =
        PrimusMetrics.getTimerContextWithAppIdTag("am.graceful_shutdown.latency", new HashMap<>());

    this.gracefulShutdown = true;
    Sleeper.sleepWithoutInterruptedException(Duration.ofMillis(primusConf.getSleepBeforeExitMs()));

    // XXX: Specialized error handling for gang failures to survive low resource clusters.
    context.emitShutdownPrimusApplicationEvent(exitCode == GANG_POLICY_FAILED.getValue());

    Thread thread = new Thread(() -> {
      long startTime = System.currentTimeMillis();
      while (context.getSchedulerExecutorManager().getContainerExecutorMap().size() > 0) {
        LOG.info(
            "Gracefully shutdown, remaining {} containers(s).",
            context.getSchedulerExecutorManager().getContainerExecutorMap().size());

        Sleeper.sleepWithoutInterruptedException(Duration.ofSeconds(3));
        if (System.currentTimeMillis() - startTime > gracefulShutdownTimeoutMs) {
          LOG.warn(
              "Graceful shutdown timeout, remaining {} container(s).",
              context.getSchedulerExecutorManager().getContainerExecutorMap().size());
          break;
        }
      }

      isStopped = true;
      context.logTimelineEvent(
          PRIMUS_APP_SHUTDOWN.name(),
          String.valueOf(gracefulShutdownLatency.stop())
      );
    });

    thread.setDaemon(true);
    thread.start();
  }

  protected List<String> getRoleNames() {
    List<String> ret = new ArrayList<>();
    for (RolePolicy rolePolicy : primusConf.getScheduler().getSchedulePolicy()
        .getOrderPolicy().getRolePolicyList()) {
      ret.add(rolePolicy.getRoleName());
    }
    return ret;
  }

  protected void scheduleRoleByOrder(Job job, List<String> roleNames) throws Exception {
    Map<String, RoleSpec> roleSpecMap = new HashMap<>(job.getSpec().getRoleSpecs());
    job.getSpec().getRoleSpecs().clear();
    int i = 0;
    for (String roleName : roleNames) {
      job.getSpec().getRoleSpecs().put(roleName, roleSpecMap.get(roleName));
      if (i == 0) {
        context.getCoreApi().createJob(job);
      } else {
        long version = context
            .getCoreApi()
            .getJob(job.getMeta().getName())
            .getMeta()
            .getVersion();
        context.getCoreApi().replaceJob(job, version);
      }
      i++;
      RoleStatus roleStatus = null;
      while (roleStatus == null
          || roleStatus.getActiveNum() + roleStatus.getFailedNum() + roleStatus.getSucceedNum()
          != roleSpecMap.get(roleName).getReplicas()) {
        roleStatus = context.getCoreApi().getJob(job.getMeta().getName()).getStatus()
            .getRoleStatuses().get(roleName);
        Thread.sleep(1000);
      }
    }
  }

  protected void checkAndUpdateDataStream() {
    if (!primusConf.hasInputManager() ||
        primusConf.getInputManager().getConfigCase() == ConfigCase.CONFIG_NOT_SET ||
        primusConf.getScheduler().getSchedulePolicy().hasOrderPolicy()) {
      // already set data to api server before scheduleRoleByOrder()
      return;
    }

    LOG.info("Add data to api server");
    Data data = ResourceUtils.buildData(primusConf);
    int maxRetryTimes = 200;
    int retryTimes = 0;

    while (true) {
      try {
        Thread.sleep(5000);
        context.getCoreApi().create(Data.class, data);
        return;

      } catch (Exception e1) {
        try {
          List<Data> dataList = context.getCoreApi().listDatas();
          if (dataList != null && !dataList.isEmpty()) {
            LOG.info("Data has been added to api server by someone");
            return;
          }
        } catch (Exception e2) {
          LOG.warn("Failed to check data, ignore", e2);
        }
        LOG.warn("Failed to create data to api server, retry", e1);
        if (++retryTimes > maxRetryTimes) {
          context.emitFailAttemptEvent(
              "Failed to set and check data",
              ApplicationExitCode.ABORT.getValue()
          );
          return;
        }
      }
    }
  }

  protected void saveHistory() {
    try {
      if (needUnregister) {
        // TODO: Maybe decouple updating application final status from saveHistory
        context.setApplicationFinalStatus(finalStatus, exitCode, unregisterDiagnostics);
        context.getHdfsStore().snapshot(true, finalStatus == FinalApplicationStatus.SUCCEEDED);
      }
    } catch (Exception e) {
      LOG.error("Failed to write history", e);
    }
  }

  protected void cleanup() {
    if (needUnregister) {
      Path stagingPath = context.getApplicationMeta().getStagingDir();
      LOG.info("Cleanup staging dir: " + stagingPath);
      try {
        FileSystem dfs = context.getApplicationMeta().getHadoopFileSystem();
        if (dfs.exists(stagingPath)) {
          dfs.delete(stagingPath, true);
        }
      } catch (IOException e) {
        LOG.warn("Failed to cleanup staging dir: " + stagingPath);
      }
    }
  }
}
