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

import static com.bytedance.primus.api.records.ExecutorState.KILLING;
import static com.bytedance.primus.api.records.ExecutorState.REGISTERED;
import static com.bytedance.primus.api.records.ExecutorState.RUNNING;
import static com.bytedance.primus.api.records.ExecutorState.STARTING;
import static com.bytedance.primus.executor.ExecutorExitCode.INVALID_INET_ADDRESS;
import static com.bytedance.primus.executor.ExecutorExitCode.SETUP_PORT_FAIL;
import static com.bytedance.primus.utils.PrimusConstants.DEFAULT_SETUP_PORT_WITH_BATCH_OR_GANG_SCHEDULER_RETRY_MAX_TIMES;

import com.bytedance.primus.api.records.ExecutorState;
import com.bytedance.primus.apiserver.proto.UtilsProto;
import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceType;
import com.bytedance.primus.common.child.ChildLauncher;
import com.bytedance.primus.common.child.ChildLauncherEventType;
import com.bytedance.primus.common.event.AsyncDispatcher;
import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.network.NetworkConfig;
import com.bytedance.primus.common.network.NetworkConfigHelper;
import com.bytedance.primus.common.retry.RetryCallback;
import com.bytedance.primus.common.retry.RetryContext;
import com.bytedance.primus.common.retry.RetryTemplate;
import com.bytedance.primus.common.service.CompositeService;
import com.bytedance.primus.common.util.RuntimeUtils;
import com.bytedance.primus.common.util.Sleeper;
import com.bytedance.primus.executor.environment.RunningEnvironment;
import com.bytedance.primus.executor.exception.PrimusExecutorException;
import com.bytedance.primus.executor.task.TaskRunnerManager;
import com.bytedance.primus.executor.task.TaskRunnerManagerEventType;
import com.bytedance.primus.executor.task.WorkerFeeder;
import com.bytedance.primus.executor.task.WorkerFeederEventType;
import com.bytedance.primus.utils.PrimusConstants;
import com.bytedance.primus.utils.timeline.NoopTimelineLogger;
import com.bytedance.primus.utils.timeline.TimelineLogger;
import java.io.IOException;
import java.net.BindException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerImpl extends CompositeService implements EventHandler<ContainerEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerImpl.class);

  private PrimusExecutorConf primusExecutorConf;
  private ExecutorContext context;
  private AsyncDispatcher dispatcher;
  private ChildLauncher workerLauncher;
  private ExecutorStatusUpdater statusUpdater;
  private Executor executor;
  private WorkerFeeder workerFeeder;
  private TaskRunnerManager taskRunnerManager;
  private volatile boolean isStopped;
  private HashSet<Integer> reservedPorts;
  private ServerSocketChannel workerFeederServerSocketChannel = null;
  private InetAddress fastestNetworkInterfaceAddress = null;

  public ContainerImpl() {
    super(ContainerImpl.class.getName());
  }

  public void init(
      PrimusExecutorConf primusExecutorConf,
      RunningEnvironment runningEnvironment
  ) throws Exception {
    this.primusExecutorConf = primusExecutorConf;
    context = new ExecutorContext(
        this.primusExecutorConf,
        RuntimeUtils.loadHadoopFileSystem(primusExecutorConf.getPrimusConf()),
        runningEnvironment
    );

    reservedPorts = new HashSet<>();
    setupPorts();

    // TODO: Make it plugable
    LOG.info("Create noop timeline listener");
    TimelineLogger timelineLogger = new NoopTimelineLogger();
    context.setTimelineLogger(timelineLogger);

    NetworkConfig netWorkConfig = NetworkConfigHelper
        .getNetworkConfig(context.getPrimusExecutorConf().getPrimusConf());
    LOG.info("container Network config: " + netWorkConfig);
    context.setNetworkConfig(netWorkConfig);

    LOG.info("dispatcher init...");
    dispatcher = new AsyncDispatcher();
    context.setDispatcher(dispatcher);

    LOG.info("executor init...");
    executor = new ExecutorImpl(context, dispatcher);
    context.setExecutor(executor);

    LOG.info("worker feeder init...");
    int workerFeederPort = -1;
    if (primusExecutorConf.getPrimusConf().getInputManager().getMaxNumWorkerFeederClients() > 0) {
      workerFeederPort = setupWorkerFeederServerSocketChannel();
    }
    workerFeeder = new WorkerFeeder(context, workerFeederServerSocketChannel, workerFeederPort);
    context.setWorkerFeeder(workerFeeder);

    LOG.info("task runner init...");
    taskRunnerManager = new TaskRunnerManager(context, dispatcher);
    addService(taskRunnerManager);
    context.setTaskRunnerManager(taskRunnerManager);

    LOG.info("worker launcher init...");
    workerLauncher = new ChildLauncher(
        primusExecutorConf.getPrimusConf().getRuntimeConf(),
        workerFeederPort);
    addService(workerLauncher);
    context.setChildLauncher(workerLauncher);

    LOG.info("status updater init...");
    statusUpdater = new ExecutorStatusUpdater(context, dispatcher);
    addService(statusUpdater);

    LOG.info("register to dispatcher");
    dispatcher.register(ContainerEventType.class, this);
    dispatcher.register(ExecutorEventType.class, executor);
    dispatcher.register(ChildLauncherEventType.class, workerLauncher);
    dispatcher.register(WorkerFeederEventType.class, workerFeeder);
    dispatcher.register(TaskRunnerManagerEventType.class, taskRunnerManager);

    addService(dispatcher);
    super.init();
  }

  @Override
  protected void serviceStart() throws Exception {
    isStopped = false;
    super.serviceStart();
  }

  @Override
  public void handle(ContainerEvent containerEvent) {
    switch (containerEvent.getType()) {
      case SHUTDOWN:
        isStopped = true;
        break;
    }
  }

  private void setupPortsForFairScheduler() throws PrimusExecutorException {
    int portBase = primusExecutorConf.getPrimusConf().getPortRange().getBase();
    int portSize = primusExecutorConf.getPrimusConf().getPortRange().getSize();

    int port = 0;
    int maxRetryTimes = primusExecutorConf.getPrimusConf().getSetupPortRetryMaxTimes();
    if (maxRetryTimes <= 0) {
      maxRetryTimes = PrimusConstants.DEFAULT_SETUP_PORT_RETRY_MAX_TIMES;
    }
    if (portSize <= 0) {
      portBase = 0;
      portSize = 0;
    }

    int retryTimes = 0;
    InetAddress inetAddress = getOrComputeFastestNetworkInterfaceAddress();
    while (retryTimes < maxRetryTimes) {
      try {
        int portNum = 0;
        for (UtilsProto.ResourceRequest r : primusExecutorConf.getExecutorSpec()
            .getResourceRequests()) {
          if (r.getResourceType() == ResourceType.PORT) {
            portNum = r.getValue();
            break;
          }
        }
        List<ServerSocket> frameworkSocketList = new ArrayList<>();
        portNum = Math.max(portNum, 1);
        for (int i = 0; i < portNum; ++i) {
          port = (int) (Math.random() * portSize) + portBase;
          while (reservedPorts.contains(port)) {
            port = (int) (Math.random() * portSize) + portBase;
          }
          reservedPorts.add(port);
          frameworkSocketList.add(new ServerSocket(port, 0, inetAddress));
          LOG.info("reserved frameworkPort[" + i + "]: "
              + frameworkSocketList.get(i).getLocalPort());
        }
        context.setFrameworkSocketList(frameworkSocketList);
        break;
      } catch (IOException e) {
        LOG.warn("failed to setup port " + port, e);
      }
      ++retryTimes;
    }

    if (retryTimes >= maxRetryTimes) {
      LOG.error("failed to setup port after [" + maxRetryTimes + "] times");
      throw new PrimusExecutorException(SETUP_PORT_FAIL.getValue());
    }

  }


  private void setupPorts() throws PrimusExecutorException {
    if (context.getPrimusExecutorConf().getPortList() == null ||
        context.getPrimusExecutorConf().getPortList().isEmpty()
    ) {
      LOG.info("random setup ports because port list is empty");
      setupPortsForFairScheduler();
      return;
    }
    setupPortsForBatchSchedulerAndGangScheduler();
    //add to reservedPortsSet, which is used during create workerFeederServerSocketChannel
    Set<Integer> portSet = context.getFrameworkSocketList().stream()
        .map(serverSocket -> serverSocket.getLocalPort())
        .collect(Collectors.toSet());
    reservedPorts.addAll(portSet);
  }

  protected void setupPortsForBatchSchedulerAndGangScheduler() {
    RetryTemplate template = RetryTemplate.builder()
        .maxAttempts(DEFAULT_SETUP_PORT_WITH_BATCH_OR_GANG_SCHEDULER_RETRY_MAX_TIMES)
        .retryOn(IOException.class)
        .retryOn(BindException.class)
        .build();
    tryToBindPortWithRetryTemplate(template);
  }

  private void tryToBindPortWithRetryTemplate(RetryTemplate template) {
    try {
      List<ServerSocket> serverSockets = template
          .execute(new RetryCallback<List<ServerSocket>, IOException>() {
            @Override
            public List<ServerSocket> doWithRetry(RetryContext context) throws IOException {
              return getServerSocketList();
            }
          });
      context.setFrameworkSocketList(serverSockets);
    } catch (IOException e) {
      int retryCount = template.getRetryContext().getRetryCount();
      throw new PrimusExecutorException(
          "Failed to setup ports with Batch/Gang Scheduler, retry:" + retryCount, e,
          SETUP_PORT_FAIL.getValue());
    }
  }

  private List<ServerSocket> getServerSocketList() throws IOException {
    InetAddress inetAddress = getOrComputeFastestNetworkInterfaceAddress();
    List<ServerSocket> frameworkSocketList = new ArrayList<>();
    for (String portStr : context.getPrimusExecutorConf().getPortList()) {
      int port = Integer.parseInt(portStr);
      try {
        ServerSocket serverSocket = new ServerSocket(port, 0, inetAddress);
        frameworkSocketList.add(serverSocket);
      } catch (IOException e) {
        Duration duration = Duration.ofSeconds(10);
        LOG.warn(
            "Bind serverSocket error! sleep for {}s, requested port: {}",
            duration.getSeconds(), port, e);
        Sleeper.sleepWithoutInterruptedException(duration);
        throw e;
      }
    }
    return frameworkSocketList;
  }

  private int setupWorkerFeederServerSocketChannel() {
    int portBase = primusExecutorConf.getPrimusConf().getPortRange().getBase();
    int portSize = primusExecutorConf.getPrimusConf().getPortRange().getSize();
    if (portSize <= 0) {
      portBase = 0;
      portSize = 0;
    }
    getOrComputeFastestNetworkInterfaceAddress();
    int retryTimes = 0;
    int port;
    for (int i = 0; i < portSize; ++i) {
      port = portBase + i;
      if (reservedPorts.contains(port)) {
        continue;
      }
      retryTimes++;
      try {
        workerFeederServerSocketChannel = ServerSocketChannel.open();
        workerFeederServerSocketChannel.socket().bind(new InetSocketAddress(port));
        workerFeederServerSocketChannel.configureBlocking(true);
        return port;
      } catch (IOException e) {
        LOG.warn("failed to setup worker feeder server socket channel on port " + port, e);
      }
    }
    if (workerFeederServerSocketChannel == null) {
      LOG.error("failed to setup worker feeder server socket channel after [" + retryTimes
          + "] times; maybe want to use workerFeederServerSocketChannel but forget define portSize:"
          + portSize);
      //throw new PrimusExecutorException(ExecutorExitCode.SETUP_PORT_FAIL.getValue());
    }
    return -1;
  }

  private void stopContainer() {
    LOG.info("stopProcess container...");
    stop();

    // XXX: Since there might be multiple background threads still pumping logs which ultimately
    // prevent synchronized log appenders to exit, we are manually shutting down LogManager here
    // though not ideal.
    LOG.info("Shutting down LogManager");
    LogManager.shutdown();
  }

  public void waitForStop() {
    Thread thread = new Thread(() -> {
      while (!isStopped) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // ignore
        }
      }
      stopContainer();
    });
    thread.setName("ContainerImpl Stop Thread");
    thread.start();
  }

  protected InetAddress getOrComputeFastestNetworkInterfaceAddress() {
    if (fastestNetworkInterfaceAddress != null) {
      return fastestNetworkInterfaceAddress;
    }
    try {
      NetworkInterface networkInterface = null;
      long maxSpeed = 0L;
      List<NetworkInterface> nets = Collections.list(NetworkInterface.getNetworkInterfaces());
      Collections.shuffle(nets, new Random());
      for (NetworkInterface ni : nets) {
        if (ni.getName().startsWith("eth") ||
            ni.getName().startsWith("enp")
        ) {
          long speed = getNetworkInterfaceSpeed(ni.getName());
          if (networkInterface == null) {
            maxSpeed = speed;
            networkInterface = ni;
            continue;
          } else if (maxSpeed < speed) {
            maxSpeed = speed;
            networkInterface = ni;
          }
        }
      }

      fastestNetworkInterfaceAddress = findFirstValidInet4Address(networkInterface.getName(),
          Collections.list(networkInterface.getInetAddresses()));
      if (fastestNetworkInterfaceAddress == null) {
        throw new PrimusExecutorException(INVALID_INET_ADDRESS.getValue());
      }
      LOG.info("Found fastestNetworkInterfaceAddress: {}.", fastestNetworkInterfaceAddress);
    } catch (SocketException e) {
      LOG.warn("Failed to get fastest network interface", e);
    }
    return fastestNetworkInterfaceAddress;
  }

  protected InetAddress findFirstValidInet4Address(String networkInterfaceName,
      List<InetAddress> inetAddresses) {
    InetAddress target = null;
    for (InetAddress inetAddress : inetAddresses) {
      if (inetAddress instanceof Inet4Address) {
        if (inetAddress.getHostAddress().equalsIgnoreCase("0.0.0.0")) {
          LOG.error("InetAddress is invalid, interface: " + networkInterfaceName);
          continue;
        }
        target = inetAddress;
      }
    }
    return target;
  }

  long getNetworkInterfaceSpeed(String networkInterfaceName) {
    long speed = -1L;
    try {
      String filename = String.format("/sys/class/net/%s/speed", networkInterfaceName);
      List<String> lines = Files.readAllLines(Paths.get(filename), StandardCharsets.UTF_8);
      if (!lines.isEmpty()) {
        speed = Long.valueOf(lines.get(0));
      }
    } catch (Exception e) {
      LOG.warn("Failed to get speed of network interface " + networkInterfaceName, e);
    } finally {
      return speed;
    }
  }

  protected void setFastestNetworkInterfaceAddress(InetAddress fastestNetworkInterfaceAddress) {
    this.fastestNetworkInterfaceAddress = fastestNetworkInterfaceAddress;
  }

  protected void setPrimusConf(PrimusExecutorConf primusConf) {
    this.primusExecutorConf = primusConf;
  }

  protected void setContext(ExecutorContext context) {
    this.context = context;
  }

  public void gracefulShutdown() {
    LOG.info("starting graceful shutdown");
    ExecutorState executorState = context.getExecutor().getExecutorState();
    if (executorState == REGISTERED || executorState == STARTING
        || executorState == RUNNING || executorState == KILLING) {
      if (dispatcher != null) {
        dispatcher.getEventHandler()
            .handle(new ExecutorEvent(ExecutorEventType.KILL, context.getExecutorId()));
      }
      while (!isStopped) {
        try {
          TimeUnit.SECONDS.sleep(10);
          LOG.info("graceful shutting down......");
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }
}

