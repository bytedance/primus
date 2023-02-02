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

import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.ExecutorState;
import com.bytedance.primus.apiserver.proto.ResourceProto.ConsulConfig;
import com.bytedance.primus.common.child.ChildLauncher;
import com.bytedance.primus.common.event.Dispatcher;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.model.records.NodeId;
import com.bytedance.primus.common.network.NetworkConfig;
import com.bytedance.primus.executor.environment.RunningEnvironment;
import com.bytedance.primus.executor.task.TaskRunnerManager;
import com.bytedance.primus.executor.task.WorkerFeeder;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.utils.timeline.TimelineLogger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import lombok.Getter;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorContext {

  private static final Logger log = LoggerFactory.getLogger(ExecutorContext.class);

  @Getter
  private final PrimusExecutorConf primusExecutorConf;

  private ExecutorId executorId;
  private Executor executor;
  private WorkerFeeder workerFeeder;
  private TaskRunnerManager taskRunnerManager;
  private List<ServerSocket> frameworkSocketList;
  private TimelineLogger timelineLogger;
  private Dispatcher dispatcher;
  private ChildLauncher childLauncher;
  private ContainerId containerId;
  private NodeId nodeId;
  private NetworkConfig networkConfig;
  private ConsulConfig consulConfig;
  private String hostname;
  private volatile boolean running;
  private RunningEnvironment runningEnvironment;

  // Runtime Environment
  @Getter
  protected final FileSystem hadoopFileSystem; // TODO: Create Primus FileSystem interface and abstract direct dependencies on HDFS.

  public ExecutorContext(
      PrimusExecutorConf primusConf,
      FileSystem hadoopFileSystem,
      RunningEnvironment runningEnvironment
  ) {
    this.running = true;
    this.primusExecutorConf = primusConf;
    this.hadoopFileSystem = hadoopFileSystem;
    this.executorId = primusConf.getExecutorId();
    this.runningEnvironment = runningEnvironment;
    init();
  }

  private void init() {
    this.hostname = getHostName();
    log.info("Executor init");
  }

  public ExecutorId getExecutorId() {
    return executorId;
  }

  public Executor getExecutor() {
    return executor;
  }

  public void setExecutor(Executor executor) {
    this.executor = executor;
  }

  public TaskRunnerManager getTaskRunnerManager() {
    return taskRunnerManager;
  }

  public void setTaskRunnerManager(TaskRunnerManager taskRunnerManager) {
    this.taskRunnerManager = taskRunnerManager;
  }

  public void setFrameworkSocketList(List<ServerSocket> frameworkSocketList) {
    this.frameworkSocketList = frameworkSocketList;
  }

  public List<ServerSocket> getFrameworkSocketList() {
    return frameworkSocketList;
  }

  public String getHostname() {
    return hostname;
  }

  public RunningEnvironment getRunningEnvironment() {
    return runningEnvironment;
  }

  public WorkerFeeder getWorkerFeeder() {
    return workerFeeder;
  }

  public void setWorkerFeeder(WorkerFeeder workerFeeder) {
    this.workerFeeder = workerFeeder;
  }

  public void setTimelineLogger(TimelineLogger timelineLogger) {
    this.timelineLogger = timelineLogger;
  }

  public TimelineLogger getTimelineLogger() {
    return timelineLogger;
  }

  public Dispatcher getDispatcher() {
    return dispatcher;
  }

  public void setDispatcher(Dispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  public ChildLauncher getChildLauncher() {
    return childLauncher;
  }

  public void setChildLauncher(ChildLauncher childLauncher) {
    this.childLauncher = childLauncher;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public void setContainerId(ContainerId containerId) {
    this.containerId = containerId;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public void setNodeId(NodeId nodeId) {
    this.nodeId = nodeId;
  }

  public NetworkConfig getNetworkConfig() {
    return networkConfig;
  }

  public void setNetworkConfig(NetworkConfig networkConfig) {
    this.networkConfig = networkConfig;
  }

  public ConsulConfig getConsulConfig() {
    return consulConfig;
  }

  public void setConsulConfig(ConsulConfig consulConfig) {
    this.consulConfig = consulConfig;
  }


  private String getHostName() {
    try {
      InetAddress ia = InetAddress.getLocalHost();
      String host = ia.getHostName();//获取计算机主机名
      return host;
    } catch (UnknownHostException e) {
      log.error("Error when getHostName", e);
      return "";
    }
  }

  public boolean isRunning() {
    return running;
  }

  public void setRunning(boolean running) {
    this.running = running;
  }

  public Duration getGracefulShutdownDuration() {
    PrimusConf primusConf = getPrimusExecutorConf().getPrimusConf();
    return executor.getExecutorState() != ExecutorState.KILLING_FORCIBLY
        ? Duration.ofMinutes(primusConf.getGracefulShutdownTimeoutMin())
        : Duration.ZERO;
  }
}
