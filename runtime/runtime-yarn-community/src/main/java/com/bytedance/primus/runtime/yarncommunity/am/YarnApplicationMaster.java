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
import com.bytedance.primus.common.model.records.FinalApplicationStatus;
import com.bytedance.primus.common.service.CompositeService;
import com.bytedance.primus.common.service.Service;
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
import com.bytedance.primus.utils.FileUtils;
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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnApplicationMaster extends CompositeService implements ApplicationMaster {

  private static final Logger LOG = LoggerFactory.getLogger(YarnApplicationMaster.class);
  protected static int STOP_NM_CLIENT_TIMEOUT = 300;

  private PrimusConf primusConf;
  private YarnAMContext yarnAMContext;

  private NMClient nmClient;
  private AMRMClient<AMRMClient.ContainerRequest> amRMClient;
  private AsyncDispatcher dispatcher;
  private AsyncDispatcher statusDispatcher;
  private URI defaultFs;
  private Path stagingDir;

  private ApiServer apiServer;

  private volatile boolean isStopped;
  private int maxAppAttempts;
  private FinalApplicationStatus finalStatus;
  private boolean needUnregister;
  private String unregisterDiagnostics;
  private String historyUrl;
  private volatile boolean gracefulShutdown = false;
  private int exitCode;
  private Set<String> distributedUris = new HashSet<>();
  private Set<String> distributedNames = new HashSet<>();

  public YarnApplicationMaster() {
    super(YarnApplicationMaster.class.getName());
    finalStatus = FinalApplicationStatus.FAILED;
    needUnregister = false;
    unregisterDiagnostics = "";
    exitCode = ApplicationExitCode.UNDEFINED.getValue();
  }

  @Override
  public void init(PrimusConf primusConf) throws Exception {
    this.primusConf = primusConf;

    LOG.info("Create AMContext");
    yarnAMContext = new YarnAMContext(primusConf);
    yarnAMContext.setApplicationMaster(this);

    LOG.info("Create event dispatchers");
    dispatcher = new AsyncDispatcher();
    yarnAMContext.setDispatcher(dispatcher);
    statusDispatcher = new AsyncDispatcher("statusDispatcher");
    yarnAMContext.setStatusDispatcher(statusDispatcher);

    LOG.info("Init FileSystem");
    defaultFs = FileSystem.getDefaultUri(yarnAMContext.getHadoopConf());
    stagingDir = new Path(yarnAMContext.getEnvs().get(STAGING_DIR_KEY));

    // Setup PrimusMetrics and timelineLogger
    // TODO: Move to a new private function
    ApplicationId appId = yarnAMContext.getAppAttemptId().getApplicationId();
    PrimusMetrics.init(appId.toString() + ".");
    historyUrl = yarnAMContext.getMonitorInfoProvider().getHistoryTrackingUrl();

    // TODO: Make it plugable
    LOG.info("Create noop timeline listener");
    TimelineLogger timelineLogger = new NoopTimelineLogger();
    yarnAMContext.setTimelineLogger(timelineLogger);
    dispatcher.register(ExecutorEventType.class, timelineLogger);

    // TODO: Misc
    maxAppAttempts = getMaxAppAttempts();

    LOG.info("Create local resources");
    Map<String, LocalResource> localResources = createLocalResources(yarnAMContext);
    yarnAMContext.setLocalResources(localResources);

    LOG.info("Create API server");
    apiServer = new ApiServer(ResourceUtils.buildApiServerConf(primusConf.getApiServerConf()));
    apiServer.start();
    yarnAMContext.setApiServerHost(apiServer.getHostName());
    yarnAMContext.setApiServerPort(apiServer.getPort());

    LOG.info("Create api client");
    Client client = new DefaultClient(apiServer.getHostName(), apiServer.getPort());
    CoreApi coreApi = new CoreApi(client);
    yarnAMContext.setCoreApi(coreApi);

    LOG.info("Create nm and rm client");
    amRMClient = AMRMClient.createAMRMClient();
    amRMClient.init(yarnAMContext.getHadoopConf());
    amRMClient.start();

    nmClient = NMClient.createNMClient();
    nmClient.init(yarnAMContext.getHadoopConf());
    nmClient.start();

    yarnAMContext.setAmRMClient(amRMClient);
    yarnAMContext.setNmClient(nmClient);

    // LOG.info("Create PrimusFlow status service");
    // PrimusFlowStatusService flowStatusService = new PrimusFlowStatusService(yarnAMContext);
    // yarnAMContext.setPrimusFlowStatusService(flowStatusService);
    // addService(flowStatusService);

    LOG.info("Create http server");
    yarnAMContext.setHttpServer(createWebAppHttpServer(yarnAMContext));

    LOG.info("Create container launcher"); // TODO: Create an interface and add to AMContext
    ContainerLauncher containerLauncher = new ContainerLauncher(yarnAMContext);
    dispatcher.register(ContainerLauncherEventType.class, containerLauncher);

    LOG.info("Create role info manager");
    RoleInfoManager roleInfoManager = new RoleInfoManager(
        yarnAMContext,
        new YarnRoleInfoFactory(primusConf));
    dispatcher.register(RoleInfoManagerEventType.class, roleInfoManager);
    yarnAMContext.setRoleInfoManager(roleInfoManager);

    LOG.info("Create failover policy manager");
    FailoverPolicyManager failoverPolicyManager = new FailoverPolicyManager(yarnAMContext);
    dispatcher.register(FailoverPolicyManagerEventType.class, failoverPolicyManager);
    yarnAMContext.setFailoverPolicyManager(failoverPolicyManager);

    LOG.info("Create schedule policy manager");
    SchedulePolicyManager schedulePolicyManager = new SchedulePolicyManager(yarnAMContext);
    dispatcher.register(SchedulePolicyManagerEventType.class, schedulePolicyManager);
    yarnAMContext.setSchedulePolicyManager(schedulePolicyManager);

    LOG.info("Create executor monitor");
    ExecutorMonitor monitor = new ExecutorMonitor(yarnAMContext);
    addService(monitor);
    yarnAMContext.setMonitor(monitor);

    LOG.info("Create scheduler executor manager");
    SchedulerExecutorManager schedulerExecutorManager = new SchedulerExecutorManager(yarnAMContext);
    yarnAMContext.setSchedulerExecutorManager(schedulerExecutorManager);
    addService(schedulerExecutorManager);

    LOG.info("Checking PONY manager");
    if (primusConf.getScheduler().getSchedulePolicy().hasPonyPolicy()) {
      LOG.info("Setting PONY manager");
      PonyManager ponyManager = new PonyManager(
          yarnAMContext,
          primusConf.getScheduler().getSchedulePolicy().getPonyPolicy().getPsRoleName());
      ponyManager.setSchedulerExecutorManager(schedulerExecutorManager);
      yarnAMContext.setPonyManager(ponyManager);

      dispatcher.register(PonyEventType.class, ponyManager);
      if (yarnAMContext.getTimelineLogger() != null) {
        dispatcher.register(PonyEventType.class, yarnAMContext.getTimelineLogger());
      }
    }

    LOG.info("Create executor tracker service");
    ExecutorTrackerService trackerService = new ExecutorTrackerService(yarnAMContext);
    addService(trackerService);
    AMService amService = new AMService(yarnAMContext);
    yarnAMContext.setAmService(amService);
    addService(amService);

    LOG.info("Create container manager");
    YarnContainerManager containerManagerService = new FairContainerManager(yarnAMContext);
    yarnAMContext.setContainerManager(containerManagerService);
    addService(containerManagerService);

    LOG.info("Create data stream manager");
    DataStreamManager dataStreamManager = new DataStreamManager(yarnAMContext);
    yarnAMContext.setDataStreamManager(dataStreamManager);
    addService(dataStreamManager);

    LOG.info("Create suspend manager");
    SuspendManager suspendManager = new SuspendManager(dataStreamManager);
    yarnAMContext.setSuspendManager(suspendManager);
    addService(suspendManager);

    LOG.info("Create progress manager");
    ProgressManager progressManager = ProgressManagerFactory.getProgressManager(yarnAMContext);
    yarnAMContext.setProgressManager(progressManager);
    addService(progressManager);

    LOG.info("Create hdfs store");
    HdfsStore hdfsStore = new HdfsStore(yarnAMContext);
    yarnAMContext.setHdfsStore(hdfsStore);
    addService(hdfsStore);

    LOG.info("Create logging listener");
    EventLoggingListener eventLoggingListener = new EventLoggingListener(yarnAMContext);
    addService(eventLoggingListener);

    LOG.info("Create ApiServerExecutorStateUpdateListener listener");
    ApiServerExecutorStateUpdateListener apiServerExecutorStateUpdateListener =
        new ApiServerExecutorStateUpdateListener(yarnAMContext);
    addService(apiServerExecutorStateUpdateListener);

    StatusEventLoggingListener statusLoggingListener =
        new StatusEventLoggingListener(yarnAMContext);
    yarnAMContext.setStatusLoggingListener(statusLoggingListener);
    addService(statusLoggingListener);

    yarnAMContext.setStatusEventWrapper(new StatusEventWrapper(yarnAMContext));
    yarnAMContext.setBlacklistTrackerOpt(createBlacklistTracker());

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

    JobController jobController = new JobController(yarnAMContext);
    yarnAMContext.setJobController(jobController);
    addService(jobController);

    DataController dataController = new DataController(yarnAMContext);
    yarnAMContext.setDataController(dataController);
    addService(dataController);

    NodeAttributeController nodeAttributeController = new NodeAttributeController(yarnAMContext);
    yarnAMContext.setNodeAttributeController(nodeAttributeController);
    addService(nodeAttributeController);

    DataSavepointController dataSavepointController = new DataSavepointController(coreApi,
        dataStreamManager);
    addService(dataSavepointController);

    this.init(yarnAMContext.getEnvConf());
  }

  @Override
  protected void serviceStart() throws Exception {
    isStopped = false;
    super.serviceStart();

    yarnAMContext.logStatusEvent(yarnAMContext.getStatusEventWrapper().buildAMStartEvent());
    Thread.sleep(15 * 1000); // Watch interface is async so sleep a while to wait for creation

    // TODO: Comment
    if (primusConf.getScheduler().getCommand().isEmpty()) {
      Job job = ResourceUtils.buildJob(yarnAMContext.getPrimusConf());
      if (yarnAMContext.getPrimusConf().getScheduler().getSchedulePolicy().hasOrderPolicy()) {
        LOG.info("Add data to api server before schedule role by order");
        Data data = ResourceUtils.buildData(primusConf);
        yarnAMContext.getCoreApi().create(Data.class, data);
        List<String> roleNames = getRoleNames();
        scheduleRoleByOrder(job, roleNames);
      } else {
        yarnAMContext.getCoreApi().createJob(job);
      }

      return;
    }

    LOG.info("Start master");
    Map<String, String> envs = new HashMap<>(System.getenv());
    envs.put(Constants.API_SERVER_RPC_HOST_ENV, apiServer.getHostName());
    envs.put(Constants.API_SERVER_RPC_PORT_ENV, String.valueOf(apiServer.getPort()));
    envs.put(PRIMUS_AM_RPC_HOST, yarnAMContext.getAmService().getHostName());
    envs.put(PRIMUS_AM_RPC_PORT, String.valueOf(yarnAMContext.getAmService().getPort()));
    MasterContext masterContext = new MasterContext(primusConf.getScheduler().getCommand(), envs);
    Master master = new Master(yarnAMContext, masterContext, dispatcher);
    yarnAMContext.setMaster(master);
    dispatcher.getEventHandler()
        .handle(new ChildLauncherEvent(ChildLauncherEventType.LAUNCH, master));
  }

  public List<String> getRoleNames() {
    List<String> ret = new ArrayList<>();
    for (RolePolicy rolePolicy : yarnAMContext.getPrimusConf().getScheduler().getSchedulePolicy()
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
        yarnAMContext.getCoreApi().createJob(job);
      } else {
        Long version = yarnAMContext.getCoreApi().getJob(job.getMeta().getName()).getMeta()
            .getVersion();
        yarnAMContext.getCoreApi().replaceJob(job, version);
      }
      i++;
      RoleStatus roleStatus = null;
      while (roleStatus == null
          || roleStatus.getActiveNum() + roleStatus.getFailedNum() + roleStatus.getSucceedNum()
          != roleSpecMap.get(roleName).getReplicas()) {
        roleStatus = yarnAMContext.getCoreApi().getJob(job.getMeta().getName()).getStatus()
            .getRoleStatuses().get(roleName);
        Thread.sleep(1000);
      }
    }
  }

  @Override
  public synchronized void handle(ApplicationMasterEvent event) {
    yarnAMContext.getTimelineLogger()
        .logEvent(PRIMUS_VERSION.name(), yarnAMContext.getVersion());
    yarnAMContext.getTimelineLogger()
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
          yarnAMContext.getProgressManager().setProgress(1f);
          gracefulShutdown(event.getGracefulShutdownTimeoutMs());
        }
        break;
      case FAIL_ATTEMPT:
        if (!gracefulShutdown) {
          this.exitCode = event.getExitCode();
          LOG.error("Handle fail attempt event, cause: " + event.getDiagnosis()
              + ", primus exit code: " + exitCode);
          finalStatus = FinalApplicationStatus.FAILED;
          needUnregister = yarnAMContext.getAppAttemptId().getAttemptId() >= maxAppAttempts;
          unregisterDiagnostics = event.getDiagnosis();
          gracefulShutdown(event.getGracefulShutdownTimeoutMs());
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
          gracefulShutdown(event.getGracefulShutdownTimeoutMs());
        }
        break;
      case SUSPEND_APP:
        ApplicationMasterSuspendAppEvent e = (ApplicationMasterSuspendAppEvent) event;
        LOG.info("Handle suspend app event, cause: " + event.getDiagnosis());
        yarnAMContext.getSuspendManager().suspend(e.getSnapshotId());
        break;
      case RESUME_APP:
        LOG.info("Handle resume app event, cause: " + event.getDiagnosis());
        yarnAMContext.getSuspendManager().resume();
        break;
      default:
        if (!gracefulShutdown) {
          this.exitCode = ApplicationExitCode.UNDEFINED.getValue();
          LOG.info("Unknown app event, primus exit code: " + exitCode);
          finalStatus = FinalApplicationStatus.UNDEFINED;
          needUnregister = yarnAMContext.getAppAttemptId().getAttemptId() >= maxAppAttempts;
          unregisterDiagnostics = event.getDiagnosis();
          gracefulShutdown(event.getGracefulShutdownTimeoutMs());
        }
        break;
    }
    if (gracefulShutdown) {
      yarnAMContext.logStatusEvent(yarnAMContext.getStatusEventWrapper()
          .buildAMEndEvent(finalStatus, exitCode, unregisterDiagnostics));
    }
  }

  public void gracefulShutdown(long timeoutMs) {
    LOG.info("start gracefulShutdown, timeout:{}", timeoutMs);
    PrimusMetrics.TimerMetric gracefulShutdownLatency =
        PrimusMetrics.getTimerContextWithOptionalPrefix("am.graceful_shutdown.latency");
    this.gracefulShutdown = true;
    try {
      Thread.sleep(primusConf.getSleepBeforeExitMs());
    } catch (InterruptedException e) {
      // ignore
    }
    dispatcher.getEventHandler()
        .handle(new ContainerManagerEvent(ContainerManagerEventType.GRACEFUL_SHUTDOWN));
    Thread thread = new Thread(() -> {
      long startTime = System.currentTimeMillis();
      boolean isTimeout = false;
      while (yarnAMContext.getSchedulerExecutorManager().getContainerExecutorMap().size() > 0
          && !isTimeout) {
        try {
          LOG.info("Running executor number "
              + yarnAMContext.getSchedulerExecutorManager().getContainerExecutorMap().size()
              + ", graceful shutdown");
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          // ignore
        }
        isTimeout = (System.currentTimeMillis() - startTime) > timeoutMs;
      }
      if (isTimeout) {
        LOG.warn("Graceful shutdown timeout, remaining container "
            + yarnAMContext.getSchedulerExecutorManager().getContainerExecutorMap().size());
      }
      yarnAMContext.getTimelineLogger().logEvent(PRIMUS_APP_SHUTDOWN.name(),
          String.valueOf(gracefulShutdownLatency.stop()));
      isStopped = true;
    });
    thread.setDaemon(true);
    thread.start();
  }

  public void abort() {
    LOG.info("Abort application");
    try {
      PrimusMetrics.TimerMetric saveHistoryLatency =
          PrimusMetrics.getTimerContextWithOptionalPrefix("am.save_history.latency");
      saveHistory();
      yarnAMContext.getTimelineLogger()
          .logEvent(PRIMUS_APP_SAVE_HISTORY.name(), String.valueOf(saveHistoryLatency.stop()));
      PrimusMetrics.TimerMetric stopComponentLatency =
          PrimusMetrics.getTimerContextWithOptionalPrefix("am.stop_component.latency");
      stop();
      yarnAMContext.getTimelineLogger()
          .logEvent(PRIMUS_APP_STOP_COMPONENT.name(), String.valueOf(stopComponentLatency.stop()));
      cleanup();
      PrimusMetrics.TimerMetric stopNMLatency =
          PrimusMetrics.getTimerContextWithOptionalPrefix("am.stop_nm.latency");
      unregisterApp();
      stopNMClientWithTimeout(nmClient);
      amRMClient.stop();
      apiServer.stop();
      yarnAMContext.getTimelineLogger()
          .logEvent(PRIMUS_APP_STOP_NM.name(), String.valueOf(stopNMLatency.stop()));
      yarnAMContext.getTimelineLogger().shutdown();
    } catch (Exception e) {
      LOG.warn("Failed to abort application", e);
    }
  }

  private void saveHistory() {
    try {
      if (needUnregister) {
        yarnAMContext.setFinalStatus(finalStatus);
        yarnAMContext.setExitCode(exitCode);
        yarnAMContext.setDiagnostic(
            unregisterDiagnostics
                + ElasticResourceContainerScheduleStrategy.getOutOfMemoryMessage());
        yarnAMContext.setFinishTime(new Date());
        yarnAMContext.getHdfsStore()
            .snapshot(true, finalStatus == FinalApplicationStatus.SUCCEEDED);
      }
    } catch (Exception e) {
      LOG.error("Failed to write history", e);
    }
  }

  public void cleanup() {
    if (needUnregister) {
      Path stagingPath = new Path(yarnAMContext.getEnvs().get(STAGING_DIR_KEY));
      LOG.info("Cleanup staging dir: " + stagingPath);
      String defaultFsPrefix = defaultFs.toString();
      try {
        FileSystem dfs = (new Path(defaultFsPrefix)).getFileSystem(yarnAMContext.getHadoopConf());
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
    int exitCode = new AMProcessExitCodeHelper(yarnAMContext.getFinalStatus()).getExitCode();
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
    URI localURI = FileUtils.resolveURI(filename.trim());
    if (!FileUtils.addDistributedUri(localURI, distributedUris, distributedNames)) {
      return;
    }

    Path path = FileUtils.getQualifiedLocalPath(localURI, yarnAMContext.getHadoopConf());
    String linkname = FileUtils.buildLinkname(path, localURI, destName, targetDir);
    if (!localURI.getScheme().equals(HDFS_SCHEME)) {
      path = new Path(defaultFs.toString() + stagingDir + "/" + linkname);
    }

    FileUtils.addResource(path, linkname, yarnAMContext.getHadoopConf(), localResources);
  }

  // TODO: Move to yarnAMContext
  private int getMaxAppAttempts() {
    int maxAppAttempts = primusConf.getMaxAppAttempts();
    if (maxAppAttempts == 0) {
      maxAppAttempts = yarnAMContext.getHadoopConf().getInt(
          YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    }
    return maxAppAttempts;
  }

  private HttpServer2 createWebAppHttpServer(AMContext amContext)
      throws URISyntaxException, IOException {
    HttpServer2 httpServer = new HttpServer2.Builder()
        .setFindPort(true)
        .setName("primus")
        .addEndpoint(new URI("http://0.0.0.0:44444"))
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
        yarnAMContext.getPrimusConf().getScheduler().getSchedulePolicy().hasOrderPolicy()) {
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
        yarnAMContext.getCoreApi().create(Data.class, data);

        return;
      } catch (Exception e1) {
        try {
          List<Data> dataList = yarnAMContext.getCoreApi().listDatas();
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
          yarnAMContext.getDispatcher().getEventHandler().handle(
              new ApplicationMasterEvent(yarnAMContext, ApplicationMasterEventType.FAIL_ATTEMPT,
                  "Failed to set and check data",
                  ApplicationExitCode.ABORT.getValue()));
          return;
        }
      }
    }
  }
}