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

package com.bytedance.primus.runtime.kubernetesnative.am;

import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesContainerConstants.PRIMUS_REMOTE_STAGING_DIR_ENV;

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
import com.bytedance.primus.am.container.ContainerManagerEvent;
import com.bytedance.primus.am.container.ContainerManagerEventType;
import com.bytedance.primus.am.controller.SuspendManager;
import com.bytedance.primus.am.datastream.DataStreamManager;
import com.bytedance.primus.am.datastream.DataStreamManagerEventType;
import com.bytedance.primus.am.eventlog.ApiServerExecutorStateUpdateListener;
import com.bytedance.primus.am.eventlog.ExecutorEventType;
import com.bytedance.primus.am.eventlog.StatusEventLoggingListener;
import com.bytedance.primus.am.eventlog.StatusEventWrapper;
import com.bytedance.primus.am.failover.FailoverPolicyManager;
import com.bytedance.primus.am.failover.FailoverPolicyManagerEventType;
import com.bytedance.primus.am.master.Master;
import com.bytedance.primus.am.master.MasterContext;
import com.bytedance.primus.am.progress.ProgressManager;
import com.bytedance.primus.am.progress.ProgressManagerFactory;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.role.RoleInfoManagerEventType;
import com.bytedance.primus.am.schedule.SchedulePolicyManager;
import com.bytedance.primus.am.schedule.SchedulePolicyManagerEventType;
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
import com.bytedance.primus.common.model.records.FinalApplicationStatus;
import com.bytedance.primus.common.service.CompositeService;
import com.bytedance.primus.common.util.Sleeper;
import com.bytedance.primus.proto.PrimusConfOuterClass;
import com.bytedance.primus.proto.PrimusConfOuterClass.OrderSchedulePolicy.RolePolicy;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusInput.InputManager.ConfigCase;
import com.bytedance.primus.runtime.kubernetesnative.am.scheduler.KubernetesContainerManager;
import com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants;
import com.bytedance.primus.utils.AMProcessExitCodeHelper;
import com.bytedance.primus.utils.ResourceUtils;
import com.bytedance.primus.utils.timeline.NoopTimelineLogger;
import com.bytedance.primus.utils.timeline.TimelineLogger;
import com.bytedance.primus.webapp.CompleteApplicationServlet;
import com.bytedance.primus.webapp.HdfsStore;
import com.bytedance.primus.webapp.StatusServlet;
import com.bytedance.primus.webapp.SuccessDataStreamServlet;
import com.bytedance.primus.webapp.SuspendServlet;
import com.bytedance.primus.webapp.SuspendStatusServlet;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServlet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesApplicationMaster extends CompositeService implements ApplicationMaster {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesApplicationMaster.class);

  private final String appId;
  private final String driverPodUniqId;

  private FinalApplicationStatus finalStatus = FinalApplicationStatus.UNDEFINED;
  private int exitCode = ApplicationExitCode.UNDEFINED.getValue();
  private boolean isStopped = false;

  private boolean needUnregister = false;
  private int maxAppAttempts;
  private Path stagingDir;

  private PrimusConf primusConf;
  private KubernetesAMContext context;

  private AsyncDispatcher dispatcher;
  private AsyncDispatcher statusDispatcher;

  private KubernetesContainerManager containerManager;
  private ApiServer apiServer; // TODO: Design and move into PrimusEngine

  private volatile boolean gracefulShutdown = false;


  public KubernetesApplicationMaster(
      String applicationId,
      String driverPodUniqId
  ) {
    super(KubernetesApplicationMaster.class.getName());

    this.appId = applicationId;
    this.driverPodUniqId = driverPodUniqId;
  }

  public void init(PrimusConf primusConf) throws Exception {
    this.primusConf = primusConf;

    LOG.info("init with conf:" + primusConf.toString());
    context = new KubernetesAMContext(primusConf, appId, driverPodUniqId);

    dispatcher = new AsyncDispatcher();
    context.setDispatcher(dispatcher);
    statusDispatcher = new AsyncDispatcher("statusDispatcher");
    context.setStatusDispatcher(statusDispatcher);
    stagingDir = new Path(context.getEnvs().get(PRIMUS_REMOTE_STAGING_DIR_ENV));
    LOG.info("Kubernetes app stagingDir:" + stagingDir);

    maxAppAttempts = primusConf.getMaxAppAttempts();
    if (maxAppAttempts <= 0) {
      maxAppAttempts = YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS;
    }

    LOG.info("Create API server");
    apiServer = new ApiServer(
        ResourceUtils.buildApiServerConf(context.getPrimusConf().getApiServerConf()),
        KubernetesConstants.DRIVER_API_SERVER_PORT
    );
    apiServer.start();
    context.setApiServerHost(apiServer.getHostName());
    context.setApiServerPort(apiServer.getPort());

    LOG.info("Create API client");
    Client client = new DefaultClient(apiServer.getHostName(), apiServer.getPort());
    CoreApi coreApi = new CoreApi(client);
    context.setCoreApi(coreApi);

    LOG.info("Create http server");
    createHttpServer(context);

    LOG.info("Create progress manager");
    ProgressManager progressManager = ProgressManagerFactory.getProgressManager(context);
    context.setProgressManager(progressManager);
    addService(progressManager);

    LOG.info("Create data stream manager");
    DataStreamManager dataStreamManager = new DataStreamManager(context);
    context.setDataStreamManager(dataStreamManager);
    addService(dataStreamManager);

    LOG.info("Create suspend manager");
    SuspendManager suspendManager = new SuspendManager(dataStreamManager);
    context.setSuspendManager(suspendManager);
    addService(suspendManager);

    LOG.info("Create role info manager");
    RoleInfoManager roleInfoManager = new RoleInfoManager(context);
    context.setRoleInfoManager(roleInfoManager);

    LOG.info("Create failover policy manager");
    FailoverPolicyManager failoverPolicyManager = new FailoverPolicyManager(context);
    context.setFailoverPolicyManager(failoverPolicyManager);

    LOG.info("Create schedule policy manager");
    SchedulePolicyManager schedulePolicyManager = new SchedulePolicyManager(context);
    context.setSchedulePolicyManager(schedulePolicyManager);

    LOG.info("Create status event wrapper");
    context.setStatusEventWrapper(new StatusEventWrapper(context));

    LOG.info("Create ApiServerExecutorStateUpdateListener");
    ApiServerExecutorStateUpdateListener apiServerExecutorStateUpdateListener =
        new ApiServerExecutorStateUpdateListener(context);
    addService(apiServerExecutorStateUpdateListener);

    /**
     * k8s treat heartbeat as ping message like yarn,
     * executorManager will make executor as failed if received no heartbeat for a long time.
     */
    LOG.info("Create executor liveliness monitor");
    ExecutorMonitor monitor = new ExecutorMonitor(context);
    addService(monitor);
    context.setMonitor(monitor);

    LOG.info("Create scheduler executor manager");
    SchedulerExecutorManager schedulerExecutorManager = new SchedulerExecutorManager(context);
    context.setSchedulerExecutorManager(schedulerExecutorManager);
    addService(schedulerExecutorManager);

    LOG.info("Create executor tracker service");
    ExecutorTrackerService trackerService = new ExecutorTrackerService(context);
    addService(trackerService);
    AMService amService = new AMService(context);
    context.setAmService(amService);
    addService(amService);

    /**
     * 依赖 schedulerExecutorManager 必须放在它的后面。
     */
    LOG.info("Create container manager");
    containerManager = creatContainerManager();
    addService(containerManager);

    LOG.info("Create hdfs store");
    HdfsStore hdfsStore = new HdfsStore(context);
    context.setHdfsStore(hdfsStore);
    addService(hdfsStore);

    KubernetesEventLogging eventLoggingListener = new KubernetesEventLogging(context);
    LOG.info("Dispatcher is registering");

    LOG.info("Create master launcher");
    ChildLauncher masterLauncher = new ChildLauncher(primusConf.getRuntimeConf());
    addService(masterLauncher);

    StatusEventLoggingListener statusLoggingListener =
        new StatusEventLoggingListener(context);
    context.setStatusLoggingListener(statusLoggingListener);
    addService(statusLoggingListener);

    context.setStatusEventWrapper(new StatusEventWrapper(context));

    context.setBlacklistTrackerOpt(createBlacklistTracker());

    dispatcher.register(ApplicationMasterEventType.class, this);
    dispatcher.register(SchedulerExecutorManagerEventType.class, schedulerExecutorManager);
    dispatcher.register(ExecutorEventType.class, eventLoggingListener);
    dispatcher.register(ContainerManagerEventType.class, containerManager);
    dispatcher.register(RoleInfoManagerEventType.class, roleInfoManager);
    dispatcher.register(FailoverPolicyManagerEventType.class, failoverPolicyManager);
    dispatcher.register(SchedulePolicyManagerEventType.class, schedulePolicyManager);
    dispatcher.register(ChildLauncherEventType.class, masterLauncher);
    dispatcher.register(DataStreamManagerEventType.class, dataStreamManager);
    dispatcher.register(SchedulerExecutorStateChangeEventType.class,
        apiServerExecutorStateUpdateListener);

    // TODO: Make it plugable
    LOG.info("Create noop timeline listener");
    TimelineLogger timelineLogger = new NoopTimelineLogger();
    context.setTimelineLogger(timelineLogger);
    dispatcher.register(ExecutorEventType.class, timelineLogger);

    addService(dispatcher);

    JobController jobController = new JobController(context);
    context.setJobController(jobController);
    addService(jobController);

    DataController dataController = new DataController(context);
    context.setDataController(dataController);
    addService(dataController);

    DataSavepointController dataSavepointController = new DataSavepointController(coreApi,
        dataStreamManager);
    addService(dataSavepointController);

    super.init();
  }

  /**
   * Primus web http server
   *
   * @throws IOException
   * @throws URISyntaxException
   */
  private void createHttpServer(
      AMContext context
  ) throws IOException, URISyntaxException {
    HttpServer2 httpServer = new HttpServer2.Builder()
        .setFindPort(true)
        .setName("primus")
        .addEndpoint(new URI("http://0.0.0.0:" + context.getPrimusUiConf().getWebUiPort()))
        .build();

    // Add servlets
    StatusServlet.setContext(context);
    CompleteApplicationServlet.setContext(context);
    SuspendServlet.setContext(context);
    SuspendStatusServlet.setContext(context);

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

    httpServer.start();
    context.setHttpServer(httpServer);
  }


  private KubernetesContainerManager creatContainerManager() {
    return new KubernetesContainerManager(context, "kubernetes-container-manager");
  }

  @Override
  protected void serviceStart() throws Exception {
    isStopped = false;
    super.serviceStart();

    // Watch interface is async so sleep a while to wait for creation
    Thread.sleep(15 * 1000);

    if (primusConf.getScheduler().getCommand().isEmpty()) {
      Job job = ResourceUtils.buildJob(context.getPrimusConf());
      if (context.getPrimusConf().getScheduler().getSchedulePolicy().hasOrderPolicy()) {
        List<String> roleNames = getRoleNames();
        scheduleRoleByOrder(job, roleNames);
      } else {
        context.getCoreApi().createJob(job);
      }
    } else {
      LOG.info("Start master");
      Map<String, String> envs = new HashMap<>(System.getenv());
      envs.put(Constants.API_SERVER_RPC_HOST_ENV, apiServer.getHostName());
      envs.put(Constants.API_SERVER_RPC_PORT_ENV, String.valueOf(apiServer.getPort()));
      MasterContext masterContext = new MasterContext(primusConf.getScheduler().getCommand(), envs);
      Master master = new Master(context, masterContext, dispatcher);
      context.setMaster(master);
      dispatcher.getEventHandler()
          .handle(new ChildLauncherEvent(ChildLauncherEventType.LAUNCH, master));
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    isStopped = true;
  }

  @Override
  public int waitForStop() throws Exception {
    // For compatibility
    checkAndUpdateDataStream();

    while (!isStopped) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    abort();
    int exitCode = new AMProcessExitCodeHelper(context.getFinalStatus()).getExitCode();
    return exitCode;
  }

  @Override
  public synchronized void handle(ApplicationMasterEvent event) {
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
          context.getProgressManager().setProgress(1f);
          gracefulShutdown(event.getGracefulShutdownTimeoutMs());
        }
        break;
      case FAIL_ATTEMPT:
        if (!gracefulShutdown) {
          this.exitCode = event.getExitCode();
          LOG.error("Handle fail attempt event, cause: " + event.getDiagnosis()
              + ", primus exit code: " + exitCode);
          finalStatus = FinalApplicationStatus.FAILED;
          needUnregister = context.getAttemptId() >= maxAppAttempts;
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
          gracefulShutdown(event.getGracefulShutdownTimeoutMs());
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
          needUnregister = context.getAttemptId() >= maxAppAttempts;
          gracefulShutdown(event.getGracefulShutdownTimeoutMs());
        }
        break;
    }
  }

  public void gracefulShutdown(long timeoutMs) {
    LOG.info("start gracefulShutdown, timeout:{}", timeoutMs);
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
      while (context.getSchedulerExecutorManager().getContainerExecutorMap().size() > 0
          && !isTimeout) {
        try {
          LOG.info("Running executor number "
              + context.getSchedulerExecutorManager().getContainerExecutorMap().size()
              + ", graceful shutdown");
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          // ignore
        }
        isTimeout = (System.currentTimeMillis() - startTime) > timeoutMs;
      }
      if (isTimeout) {
        LOG.warn("Graceful shutdown timeout, remaining container "
            + context.getSchedulerExecutorManager().getContainerExecutorMap().size());
      }
      isStopped = true;
    });
    thread.setDaemon(true);
    thread.start();
  }

  private void abort() {
    LOG.info("Abort application");
    stop();
    cleanup();
  }

  private void cleanup() {
    try {
      if (needUnregister) {
        context.setFinalStatus(finalStatus);
        context.getHdfsStore().snapshot(true, finalStatus == FinalApplicationStatus.SUCCEEDED);
        Path stagingPath = stagingDir;
        LOG.info("Cleanup staging dir: " + stagingPath);
        try {
          FileSystem dfs = context.getHadoopFileSystem();
          if (!stagingPath.getName().startsWith("primus-")) {
            throw new IllegalStateException("kubernetes only delete staging folder under primus-");
          }
          if (dfs.exists(stagingPath)) {
            dfs.delete(stagingPath, true);
          }
        } catch (IOException e) {
          LOG.warn("Failed to cleanup staging dir: " + stagingPath);
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to unregister application", e);
    }
    apiServer.stop();
  }


  public List<String> getRoleNames() {

    List<String> ret = new ArrayList<>();
    for (RolePolicy rolePolicy : context.getPrimusConf().getScheduler().getSchedulePolicy()
        .getOrderPolicy().getRolePolicyList()) {
      ret.add(rolePolicy.getRoleName());
    }
    return ret;
  }

  // Creates the instances of each role gradually with order of roleNames
  public void scheduleRoleByOrder(Job job, List<String> roleNames) throws Exception {
    // Backup and clear roleSpecMap
    Map<String, RoleSpec> roleSpecMap = new HashMap<>();
    for (Map.Entry<String, RoleSpec> entry : job.getSpec().getRoleSpecs().entrySet()) {
      roleSpecMap.put(entry.getKey(), entry.getValue());
    }
    job.getSpec().getRoleSpecs().clear();

    // Start instance creation for each role
    boolean created = false;
    for (String roleName : roleNames) {
      job.getSpec().getRoleSpecs().put(roleName, roleSpecMap.get(roleName));
      // TODO: [API] move this logic to API server
      if (!created) {
        context.getCoreApi().createJob(job);
        created = true;
      } else {
        Long version = context.getCoreApi()
            .getJob(job.getMeta().getName())
            .getMeta()
            .getVersion();
        context.getCoreApi().replaceJob(job, version);
      }

      // Wait till the instances are created
      Sleeper.loop(Duration.ofSeconds(1L), () -> {
        RoleStatus status = context.getCoreApi()
            .getJob(job.getMeta().getName())
            .getStatus()
            .getRoleStatuses()
            .get(roleName);
        int allocated = status.getActiveNum() + status.getFailedNum() + status.getSucceedNum();
        return allocated == roleSpecMap.get(roleName).getReplicas();
      });
    }
  }

  private Optional<BlacklistTracker> createBlacklistTracker() {
    PrimusConfOuterClass.BlacklistConfig blacklistConfig = primusConf.getBlacklistConfig();
    if (blacklistConfig.getEnabled()) {
      LOG.info("Blacklist is enabled");
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
          maxBlacklistNode));
    } else {
      LOG.info("Blacklist is not enabled");
      return Optional.empty();
    }
  }

  private void checkAndUpdateDataStream() {
    if (!primusConf.hasInputManager() ||
        primusConf.getInputManager().getConfigCase() == ConfigCase.CONFIG_NOT_SET) {
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
