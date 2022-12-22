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

import static com.bytedance.primus.am.ApplicationExitCode.GANG_POLICY_FAILED;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_APPLICATION_MASTER_EVENT;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_APP_SAVE_HISTORY;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_APP_SHUTDOWN;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_APP_STOP_COMPONENT;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_APP_STOP_NM;
import static com.bytedance.primus.common.event.TimelineEventType.PRIMUS_VERSION;
import static com.bytedance.primus.utils.PrimusConstants.HDFS_SCHEME;
import static com.bytedance.primus.utils.PrimusConstants.LOG4J_PROPERTIES;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_AM_RPC_HOST;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_AM_RPC_PORT;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CONF;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CONF_PATH;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_JAR;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_JAR_PATH;
import static com.bytedance.primus.utils.PrimusConstants.STAGING_DIR_KEY;

import com.bytedance.blacklist.BlacklistTracker;
import com.bytedance.blacklist.BlacklistTrackerImpl;
import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.AMService;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.ApplicationMaster;
import com.bytedance.primus.am.ApplicationMasterEvent;
import com.bytedance.primus.am.ApplicationMasterEventType;
import com.bytedance.primus.am.ApplicationMasterSuspendAppEvent;
import com.bytedance.primus.am.ExecutorMonitor;
import com.bytedance.primus.am.ExecutorTrackerService;
import com.bytedance.primus.am.apiserver.DataController;
import com.bytedance.primus.am.apiserver.DataSavepointController;
import com.bytedance.primus.am.apiserver.JobController;
import com.bytedance.primus.am.apiserver.NodeAttributeController;
import com.bytedance.primus.am.container.ContainerManagerEvent;
import com.bytedance.primus.am.container.ContainerManagerEventType;
import com.bytedance.primus.am.controller.SuspendManager;
import com.bytedance.primus.am.datastream.DataStreamManager;
import com.bytedance.primus.am.datastream.DataStreamManagerEventType;
import com.bytedance.primus.am.eventlog.ApiServerExecutorStateUpdateListener;
import com.bytedance.primus.am.eventlog.EventLoggingListener;
import com.bytedance.primus.am.eventlog.ExecutorEventType;
import com.bytedance.primus.am.eventlog.StatusEventLoggingListener;
import com.bytedance.primus.am.eventlog.StatusEventType;
import com.bytedance.primus.am.eventlog.StatusEventWrapper;
import com.bytedance.primus.am.failover.FailoverPolicyManager;
import com.bytedance.primus.am.failover.FailoverPolicyManagerEventType;
import com.bytedance.primus.am.master.Master;
import com.bytedance.primus.am.master.MasterContext;
import com.bytedance.primus.am.progress.ProgressManager;
import com.bytedance.primus.am.progress.ProgressManagerFactory;
import com.bytedance.primus.am.psonyarn.PonyEventType;
import com.bytedance.primus.am.psonyarn.PonyManager;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.role.RoleInfoManagerEventType;
import com.bytedance.primus.am.schedule.SchedulePolicyManager;
import com.bytedance.primus.am.schedule.SchedulePolicyManagerEventType;
import com.bytedance.primus.am.schedule.strategy.impl.ElasticResourceContainerScheduleStrategy;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManager;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerEventType;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorStateChangeEventType;
import com.bytedance.primus.apiserver.client.Client;
import com.bytedance.primus.apiserver.client.DefaultClient;
import com.bytedance.primus.apiserver.client.apis.CoreApi;
import com.bytedance.primus.apiserver.client.models.Data;
import com.bytedance.primus.apiserver.client.models.Job;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.apiserver.records.RoleStatus;
import com.bytedance.primus.apiserver.service.ApiServer;
import com.bytedance.primus.apiserver.utils.Constants;
import com.bytedance.primus.common.child.ChildLauncher;
import com.bytedance.primus.common.child.ChildLauncherEvent;
import com.bytedance.primus.common.child.ChildLauncherEventType;
import com.bytedance.primus.common.event.AsyncDispatcher;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.model.records.ApplicationId;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.model.records.FinalApplicationStatus;
import com.bytedance.primus.common.service.CompositeService;
import com.bytedance.primus.common.service.Service;
import com.bytedance.primus.common.util.Sleeper;
import com.bytedance.primus.proto.PrimusConfOuterClass;
import com.bytedance.primus.proto.PrimusConfOuterClass.OrderSchedulePolicy.RolePolicy;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusInput.InputManager.ConfigCase;
import com.bytedance.primus.runtime.yarncommunity.am.container.YarnContainerManager;
import com.bytedance.primus.runtime.yarncommunity.am.container.YarnRoleInfoFactory;
import com.bytedance.primus.runtime.yarncommunity.am.container.launcher.ContainerLauncher;
import com.bytedance.primus.runtime.yarncommunity.am.container.launcher.ContainerLauncherEventType;
import com.bytedance.primus.runtime.yarncommunity.am.container.scheduler.fair.FairContainerManager;
import com.bytedance.primus.runtime.yarncommunity.utils.YarnConvertor;
import com.bytedance.primus.utils.AMProcessExitCodeHelper;
import com.bytedance.primus.utils.FileSystemUtils;
import com.bytedance.primus.utils.ResourceUtils;
import com.bytedance.primus.utils.timeline.NoopTimelineLogger;
import com.bytedance.primus.utils.timeline.TimelineLogger;
import com.bytedance.primus.webapp.CompleteApplicationServlet;
import com.bytedance.primus.webapp.HdfsStore;
import com.bytedance.primus.webapp.StatusServlet;
import com.bytedance.primus.webapp.SuccessDataStreamServlet;
import com.bytedance.primus.webapp.SuspendServlet;
import com.bytedance.primus.webapp.SuspendStatusServlet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.servlet.http.HttpServlet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnApplicationMaster extends CompositeService implements ApplicationMaster {

  private static final Logger LOG = LoggerFactory.getLogger(YarnApplicationMaster.class);
  protected static int STOP_NM_CLIENT_TIMEOUT = 300;

  private final ContainerId containerId;

  private PrimusConf primusConf;
  private YarnAMContext context;

  private NMClient nmClient;
  private AMRMClient<AMRMClient.ContainerRequest> amRMClient;
  private AsyncDispatcher dispatcher;
  private AsyncDispatcher statusDispatcher;
  private Path stagingDir;

  private ApiServer apiServer;

  private volatile boolean isStopped;
  private FinalApplicationStatus finalStatus = FinalApplicationStatus.UNDEFINED;
  private boolean needUnregister = false;
  private String unregisterDiagnostics = "";
  private String historyUrl;
  private volatile boolean gracefulShutdown = false;
  private int exitCode = ApplicationExitCode.UNDEFINED.getValue();
  private Set<String> distributedUris = new HashSet<>();
  private Set<String> distributedNames = new HashSet<>();

  public YarnApplicationMaster(ContainerId containerId) {
    super(YarnApplicationMaster.class.getName());
    this.containerId = containerId;
  }

  public void init(PrimusConf primusConf) throws Exception {
    this.primusConf = primusConf;

    LOG.info("Create AMContext");
    context = new YarnAMContext(primusConf, containerId);

    LOG.info("Create event dispatchers");
    dispatcher = new AsyncDispatcher();
    context.setDispatcher(dispatcher);
    statusDispatcher = new AsyncDispatcher("statusDispatcher");
    context.setStatusDispatcher(statusDispatcher);

    LOG.info("Init FileSystem");
    stagingDir = new Path(context.getEnvs().get(STAGING_DIR_KEY));

    // TODO: Move to a new private function
    ApplicationId appId = context.getAppAttemptId().getApplicationId();
    PrimusMetrics.init(
        primusConf.getRuntimeConf(),
        appId.toString());
    historyUrl = context.getMonitorInfoProvider().getHistoryTrackingUrl();

    // TODO: Make it plugable
    // Setup timelineLogger
    LOG.info("Create noop timeline listener");
    TimelineLogger timelineLogger = new NoopTimelineLogger();
    context.setTimelineLogger(timelineLogger);
    dispatcher.register(ExecutorEventType.class, timelineLogger);

    LOG.info("Create local resources");
    Map<String, LocalResource> localResources = createLocalResources(context);
    context.setLocalResources(localResources);

    LOG.info("Create API server");
    apiServer = new ApiServer(ResourceUtils.buildApiServerConf(primusConf.getApiServerConf()));
    apiServer.start();
    context.setApiServerHost(apiServer.getHostName());
    context.setApiServerPort(apiServer.getPort());

    LOG.info("Create api client");
    Client client = new DefaultClient(apiServer.getHostName(), apiServer.getPort());
    CoreApi coreApi = new CoreApi(client);
    context.setCoreApi(coreApi);

    LOG.info("Create nm and rm client");
    amRMClient = AMRMClient.createAMRMClient();
    amRMClient.init(context.getYarnConfiguration());
    amRMClient.start();

    nmClient = NMClient.createNMClient();
    nmClient.init(context.getYarnConfiguration());
    nmClient.start();

    context.setAmRMClient(amRMClient);
    context.setNmClient(nmClient);

    LOG.info("Create http server");
    context.setHttpServer(createWebAppHttpServer(context));

    LOG.info("Create container launcher"); // TODO: Create an interface and add to AMContext
    ContainerLauncher containerLauncher = new ContainerLauncher(context);
    dispatcher.register(ContainerLauncherEventType.class, containerLauncher);

    LOG.info("Create role info manager");
    RoleInfoManager roleInfoManager = new RoleInfoManager(
        context,
        new YarnRoleInfoFactory(primusConf));
    dispatcher.register(RoleInfoManagerEventType.class, roleInfoManager);
    context.setRoleInfoManager(roleInfoManager);

    LOG.info("Create failover policy manager");
    FailoverPolicyManager failoverPolicyManager = new FailoverPolicyManager(context);
    dispatcher.register(FailoverPolicyManagerEventType.class, failoverPolicyManager);
    context.setFailoverPolicyManager(failoverPolicyManager);

    LOG.info("Create schedule policy manager");
    SchedulePolicyManager schedulePolicyManager = new SchedulePolicyManager(context);
    dispatcher.register(SchedulePolicyManagerEventType.class, schedulePolicyManager);
    context.setSchedulePolicyManager(schedulePolicyManager);

    LOG.info("Create executor monitor");
    ExecutorMonitor monitor = new ExecutorMonitor(context);
    addService(monitor);
    context.setMonitor(monitor);

    LOG.info("Create scheduler executor manager");
    SchedulerExecutorManager schedulerExecutorManager = new SchedulerExecutorManager(context);
    context.setSchedulerExecutorManager(schedulerExecutorManager);
    addService(schedulerExecutorManager);

    LOG.info("Checking PONY manager");
    if (primusConf.getScheduler().getSchedulePolicy().hasPonyPolicy()) {
      LOG.info("Setting PONY manager");
      PonyManager ponyManager = new PonyManager(
          context,
          primusConf.getScheduler().getSchedulePolicy().getPonyPolicy().getPsRoleName());
      ponyManager.setSchedulerExecutorManager(schedulerExecutorManager);
      context.setPonyManager(ponyManager);

      dispatcher.register(PonyEventType.class, ponyManager);
      if (context.getTimelineLogger() != null) {
        dispatcher.register(PonyEventType.class, context.getTimelineLogger());
      }
    }

    LOG.info("Create executor tracker service");
    ExecutorTrackerService trackerService = new ExecutorTrackerService(context);
    addService(trackerService);
    AMService amService = new AMService(context);
    context.setAmService(amService);
    addService(amService);

    LOG.info("Create container manager");
    YarnContainerManager containerManagerService = new FairContainerManager(context);
    context.setContainerManager(containerManagerService);
    addService(containerManagerService);

    LOG.info("Create data stream manager");
    DataStreamManager dataStreamManager = new DataStreamManager(context);
    context.setDataStreamManager(dataStreamManager);
    addService(dataStreamManager);

    LOG.info("Create suspend manager");
    SuspendManager suspendManager = new SuspendManager(dataStreamManager);
    context.setSuspendManager(suspendManager);
    addService(suspendManager);

    LOG.info("Create progress manager");
    ProgressManager progressManager = ProgressManagerFactory.getProgressManager(context);
    context.setProgressManager(progressManager);
    addService(progressManager);

    LOG.info("Create hdfs store");
    HdfsStore hdfsStore = new HdfsStore(context);
    context.setHdfsStore(hdfsStore);
    addService(hdfsStore);

    LOG.info("Create logging listener");
    EventLoggingListener eventLoggingListener = new EventLoggingListener(context);
    addService(eventLoggingListener);

    LOG.info("Create ApiServerExecutorStateUpdateListener listener");
    ApiServerExecutorStateUpdateListener apiServerExecutorStateUpdateListener =
        new ApiServerExecutorStateUpdateListener(context);
    addService(apiServerExecutorStateUpdateListener);

    StatusEventLoggingListener statusLoggingListener =
        new StatusEventLoggingListener(context);
    context.setStatusLoggingListener(statusLoggingListener);
    addService(statusLoggingListener);

    context.setStatusEventWrapper(new StatusEventWrapper(context));
    context.setBlacklistTrackerOpt(createBlacklistTracker());

    ChildLauncher masterLauncher = new ChildLauncher(primusConf.getRuntimeConf());
    addService(masterLauncher);

    LOG.info("Dispatcher is registering");
    statusDispatcher.register(StatusEventType.class, statusLoggingListener);
    dispatcher.register(ApplicationMasterEventType.class, this);
    dispatcher.register(ApplicationMasterEventType.class, eventLoggingListener);
    dispatcher.register(ExecutorEventType.class, eventLoggingListener);
    dispatcher.register(SchedulerExecutorStateChangeEventType.class,
        apiServerExecutorStateUpdateListener);
    dispatcher.register(SchedulerExecutorManagerEventType.class, schedulerExecutorManager);
    dispatcher.register(ContainerManagerEventType.class, containerManagerService);
    dispatcher.register(ChildLauncherEventType.class, masterLauncher);
    dispatcher.register(DataStreamManagerEventType.class, dataStreamManager);

    // TODO: Check when is the best timing to add dispatcher to services
    addService(dispatcher);
    addService(statusDispatcher);

    JobController jobController = new JobController(context);
    context.setJobController(jobController);
    addService(jobController);

    DataController dataController = new DataController(context);
    context.setDataController(dataController);
    addService(dataController);

    NodeAttributeController nodeAttributeController = new NodeAttributeController(context);
    context.setNodeAttributeController(nodeAttributeController);
    addService(nodeAttributeController);

    DataSavepointController dataSavepointController = new DataSavepointController(coreApi,
        dataStreamManager);
    addService(dataSavepointController);

    this.init();
  }

  @Override
  protected void serviceStart() throws Exception {
    isStopped = false;
    super.serviceStart();

    context.logStatusEvent(context.getStatusEventWrapper().buildAMStartEvent());
    Thread.sleep(15 * 1000); // Watch interface is async so sleep a while to wait for creation

    // TODO: Comment
    if (primusConf.getScheduler().getCommand().isEmpty()) {
      Job job = ResourceUtils.buildJob(context.getPrimusConf());
      if (context.getPrimusConf().getScheduler().getSchedulePolicy().hasOrderPolicy()) {
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
    Map<String, String> envs = new HashMap<>(System.getenv());
    envs.put(Constants.API_SERVER_RPC_HOST_ENV, apiServer.getHostName());
    envs.put(Constants.API_SERVER_RPC_PORT_ENV, String.valueOf(apiServer.getPort()));
    envs.put(PRIMUS_AM_RPC_HOST, context.getAmService().getHostName());
    envs.put(PRIMUS_AM_RPC_PORT, String.valueOf(context.getAmService().getPort()));
    MasterContext masterContext = new MasterContext(primusConf.getScheduler().getCommand(), envs);
    Master master = new Master(context, masterContext, dispatcher);
    context.setMaster(master);
    dispatcher.getEventHandler()
        .handle(new ChildLauncherEvent(ChildLauncherEventType.LAUNCH, master));
  }

  public List<String> getRoleNames() {
    List<String> ret = new ArrayList<>();
    for (RolePolicy rolePolicy : context.getPrimusConf().getScheduler().getSchedulePolicy()
        .getOrderPolicy().getRolePolicyList()) {
      ret.add(rolePolicy.getRoleName());
    }
    return ret;
  }

  public void scheduleRoleByOrder(Job job, List<String> roleNames) throws Exception {
    Map<String, RoleSpec> roleSpecMap = new HashMap<>();
    for (Map.Entry<String, RoleSpec> entry : job.getSpec().getRoleSpecs().entrySet()) {
      roleSpecMap.put(entry.getKey(), entry.getValue());
    }
    job.getSpec().getRoleSpecs().clear();
    int i = 0;
    for (String roleName : roleNames) {
      job.getSpec().getRoleSpecs().put(roleName, roleSpecMap.get(roleName));
      if (i == 0) {
        context.getCoreApi().createJob(job);
      } else {
        Long version = context.getCoreApi().getJob(job.getMeta().getName()).getMeta()
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

  @Override
  public synchronized void handle(ApplicationMasterEvent event) {
    context.getTimelineLogger()
        .logEvent(PRIMUS_VERSION.name(), context.getVersion());
    context.getTimelineLogger()
        .logEvent(PRIMUS_APPLICATION_MASTER_EVENT.name(), event.getType().name());
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
          needUnregister = context.getAppAttemptId().getAttemptId() >= context.getMaxAppAttempts();
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
          needUnregister = context.getAppAttemptId().getAttemptId() >= context.getMaxAppAttempts();
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

    // Specialized error handling for gang failures to survive low resource clusters.
    dispatcher.getEventHandler().handle(exitCode == GANG_POLICY_FAILED.getValue()
        ? new ContainerManagerEvent(ContainerManagerEventType.FORCIBLY_SHUTDOWN)
        : new ContainerManagerEvent(ContainerManagerEventType.GRACEFUL_SHUTDOWN));

    Thread thread = new Thread(() -> {
      long startTime = System.currentTimeMillis();
      while (context.getSchedulerExecutorManager().getContainerExecutorMap().size() > 0) {
        LOG.info(
            "Shutting down Primus application, remaining {} containers(s).",
            context.getSchedulerExecutorManager().getContainerExecutorMap().size());

        Sleeper.sleepWithoutInterruptedException(Duration.ofSeconds(3));
        if (System.currentTimeMillis() - startTime > gracefulShutdownTimeoutMs) {
          LOG.warn(
              "Shutting down Primus timeout, remaining {} container(s).",
              context.getSchedulerExecutorManager().getContainerExecutorMap().size());
          break;
        }
      }

      isStopped = true;
      context.getTimelineLogger().logEvent(
          PRIMUS_APP_SHUTDOWN.name(),
          String.valueOf(gracefulShutdownLatency.stop())
      );
    });

    thread.setDaemon(true);
    thread.start();
  }

  public void abort() {
    LOG.info("Abort application");
    try {
      PrimusMetrics.TimerMetric saveHistoryLatency =
          PrimusMetrics.getTimerContextWithAppIdTag("am.save_history.latency", new HashMap<>());
      saveHistory();
      context.getTimelineLogger()
          .logEvent(PRIMUS_APP_SAVE_HISTORY.name(), String.valueOf(saveHistoryLatency.stop()));
      PrimusMetrics.TimerMetric stopComponentLatency =
          PrimusMetrics.getTimerContextWithAppIdTag("am.stop_component.latency", new HashMap<>());
      stop();
      context.getTimelineLogger()
          .logEvent(PRIMUS_APP_STOP_COMPONENT.name(), String.valueOf(stopComponentLatency.stop()));
      cleanup();
      PrimusMetrics.TimerMetric stopNMLatency =
          PrimusMetrics.getTimerContextWithAppIdTag("am.stop_nm.latency", new HashMap<>());
      unregisterApp();
      stopNMClientWithTimeout(nmClient);
      amRMClient.stop();
      apiServer.stop();
      context.getTimelineLogger()
          .logEvent(PRIMUS_APP_STOP_NM.name(), String.valueOf(stopNMLatency.stop()));
      context.getTimelineLogger().shutdown();
    } catch (Exception e) {
      LOG.warn("Failed to abort application", e);
    }
  }

  private void saveHistory() {
    try {
      if (needUnregister) {
        context.setFinalStatus(finalStatus);
        context.setExitCode(exitCode);
        context.setDiagnostic(
            unregisterDiagnostics
                + ElasticResourceContainerScheduleStrategy.getOutOfMemoryMessage());
        context.setFinishTime(new Date());
        context.getHdfsStore()
            .snapshot(true, finalStatus == FinalApplicationStatus.SUCCEEDED);
      }
    } catch (Exception e) {
      LOG.error("Failed to write history", e);
    }
  }

  public void cleanup() {
    if (needUnregister) {
      Path stagingPath = new Path(context.getEnvs().get(STAGING_DIR_KEY));
      LOG.info("Cleanup staging dir: " + stagingPath);
      try {
        FileSystem dfs = context.getHadoopFileSystem();
        if (dfs.exists(stagingPath)) {
          dfs.delete(stagingPath, true);
        }
      } catch (IOException e) {
        LOG.warn("Failed to cleanup staging dir: " + stagingPath);
      }
    }
  }

  public void unregisterApp() throws YarnException, IOException {
    try {
      if (needUnregister) {
        amRMClient.unregisterApplicationMaster(
            YarnConvertor.toYarnFinalApplicationStatus(finalStatus),
            unregisterDiagnostics, historyUrl);
      }
    } catch (YarnException | IOException e) {
      LOG.error("Failed to unregister application", e);
    }
  }

  @Override
  public int waitForStop() {
    // For compatibility
    checkAndUpdateData();

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


  protected boolean stopNMClientWithTimeout(NMClient nodeManagerClient) {
    try {
      ListenableFuture<?> closeFuture =
          MoreExecutors
              .listeningDecorator(
                  Executors.newSingleThreadExecutor(
                      new ThreadFactoryBuilder().setDaemon(true).build()))
              .submit(() -> nodeManagerClient.stop());

      closeFuture.addListener(
          () -> LOG.info("nmClient stopped."),
          MoreExecutors.newDirectExecutorService());
      // Yarn default heartbeat interval = 600s
      closeFuture.get(STOP_NM_CLIENT_TIMEOUT, TimeUnit.SECONDS);
      return true;
    } catch (TimeoutException e) {
      LOG.error("Stop nmClient TimeoutException in " + STOP_NM_CLIENT_TIMEOUT + "s", e);
    } catch (Exception e) {
      LOG.error("Error when stop nmClient in {}s", STOP_NM_CLIENT_TIMEOUT);
    }
    return false;
  }

  private Optional<BlacklistTracker> createBlacklistTracker() {
    PrimusConfOuterClass.BlacklistConfig blacklistConfig = primusConf.getBlacklistConfig();
    int maxFailedTaskPerContainer = blacklistConfig.getMaxFailedTaskPerContainer();
    maxFailedTaskPerContainer = (maxFailedTaskPerContainer == 0) ? 3 : maxFailedTaskPerContainer;
    int maxFailedContainerPerNode = blacklistConfig.getMaxFailedContainerPerNode();
    maxFailedContainerPerNode = (maxFailedContainerPerNode == 0) ? 2 : maxFailedContainerPerNode;
    long blacklistTimeoutMillis = blacklistConfig.getBlacklistTimeoutMillis();
    blacklistTimeoutMillis = (blacklistTimeoutMillis == 0) ? 3600000 : blacklistTimeoutMillis;
    int maxBlacklistContainer = blacklistConfig.getMaxBlacklistContainer();
    maxBlacklistContainer = (maxBlacklistContainer == 0) ? 50 : maxBlacklistContainer;
    int maxBlacklistNode = blacklistConfig.getMaxBlacklistNode();
    maxBlacklistNode = (maxBlacklistNode == 0) ? 20 : maxBlacklistNode;
    return Optional.of(new BlacklistTrackerImpl(
        maxFailedTaskPerContainer,
        maxFailedContainerPerNode,
        blacklistTimeoutMillis,
        maxBlacklistContainer,
        maxBlacklistNode,
        blacklistConfig.getEnabled()));
  }

  public void addExtraService(Service service) {
    addService(service);
  }

  private Map<String, LocalResource> createLocalResources(AMContext context)
      throws IOException, URISyntaxException {
    Map<String, LocalResource> localResources = new HashMap<>();
    addToResource(
        PRIMUS_CONF_PATH + "/" + LOG4J_PROPERTIES,
        LOG4J_PROPERTIES,
        PRIMUS_CONF_PATH,
        localResources);
    addToResource(
        PRIMUS_CONF_PATH + "/" + PRIMUS_CONF,
        PRIMUS_CONF,
        PRIMUS_CONF_PATH,
        localResources);
    addToResource(
        PRIMUS_JAR_PATH + "/" + PRIMUS_JAR,
        PRIMUS_JAR,
        PRIMUS_JAR_PATH,
        localResources);
    for (String pathStr : context.getPrimusConf().getFilesList()) {
      addToResource(pathStr, null, null, localResources);
    }
    return localResources;
  }

  private void addToResource(
      String filename,
      String destName,
      String targetDir,
      Map<String, LocalResource> localResources
  ) throws IOException, URISyntaxException {
    URI localURI = FileSystemUtils.resolveURI(filename.trim());
    if (!FileSystemUtils.addDistributedUri(localURI, distributedUris, distributedNames)) {
      return;
    }

    FileSystem fs = context.getHadoopFileSystem();
    Path path = FileSystemUtils.getQualifiedLocalPath(localURI, fs.getConf());
    String linkname = FileSystemUtils.buildLinkname(path, localURI, destName, targetDir);
    if (!localURI.getScheme().equals(HDFS_SCHEME)) {
      path = new Path(fs.getUri().toString() + stagingDir + "/" + linkname);
    }

    FileSystemUtils.addResource(fs, path, linkname, localResources);
  }

  private HttpServer2 createWebAppHttpServer(
      AMContext amContext
  ) throws URISyntaxException, IOException {
    HttpServer2 httpServer = new HttpServer2.Builder()
        .setFindPort(true)
        .setName("primus")
        .addEndpoint(new URI("http://0.0.0.0:" + amContext.getPrimusUiConf().getWebUiPort()))
        .build();

    new HashMap<String, Class<? extends HttpServlet>>() {{
      put("/webapps/primus/status.json", StatusServlet.class);
      put("/webapps/primus/kill", CompleteApplicationServlet.class);
      put("/webapps/primus/fail", CompleteApplicationServlet.class);
      put("/webapps/primus/success", CompleteApplicationServlet.class);
      put("/webapps/primus/suspend", SuspendServlet.class);
      put("/webapps/primus/success_datastream", SuccessDataStreamServlet.class);
      put("/webapps/primus/suspend/status", SuspendStatusServlet.class);
    }}.forEach((path, clazz) -> httpServer.addInternalServlet(
        null /* name */, path, clazz, false /* requireAuth */));

    // Start the server
    httpServer.start();

    StatusServlet.setContext(amContext);
    CompleteApplicationServlet.setContext(amContext);
    SuspendServlet.setContext(amContext);
    SuspendStatusServlet.setContext(amContext);
    SuccessDataStreamServlet.setContext(amContext);

    return httpServer;
  }


  private void checkAndUpdateData() {
    if (!primusConf.hasInputManager() ||
        primusConf.getInputManager().getConfigCase() == ConfigCase.CONFIG_NOT_SET ||
        context.getPrimusConf().getScheduler().getSchedulePolicy().hasOrderPolicy()) {
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
        retryTimes += 1;
        if (retryTimes > maxRetryTimes) {
          context.getDispatcher().getEventHandler().handle(
              new ApplicationMasterEvent(context, ApplicationMasterEventType.FAIL_ATTEMPT,
                  "Failed to set and check data",
                  ApplicationExitCode.ABORT.getValue()));
          return;
        }
      }
    }
  }
}