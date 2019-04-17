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
import com.bytedance.primus.apiserver.proto.UtilsProto.StreamingInputPolicy.StreamingMode;
import com.bytedance.primus.common.child.ChildLauncherEvent;
import com.bytedance.primus.common.child.ChildLauncherEventType;
import com.bytedance.primus.common.event.Dispatcher;
import com.bytedance.primus.common.state.InvalidStateTransitonException;
import com.bytedance.primus.common.state.MultipleArcTransition;
import com.bytedance.primus.common.state.SingleArcTransition;
import com.bytedance.primus.common.state.StateMachine;
import com.bytedance.primus.common.state.StateMachineFactory;
import com.bytedance.primus.executor.task.TaskRunnerManagerEvent;
import com.bytedance.primus.executor.task.TaskRunnerManagerEventType;
import com.bytedance.primus.executor.worker.Worker;
import com.bytedance.primus.executor.worker.WorkerContext;
import java.util.EnumSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorImpl implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorImpl.class);

  private final Lock readLock;
  private final Lock writeLock;
  private ExecutorContext executorContext;
  private Dispatcher dispatcher;
  private WorkerContext workerContext;
  private int exitCode;
  private String failMsg;
  volatile boolean needMoreTask;
  private String yarnRegistryRootPath;
  // update interval in seconds
  private int registryUpdateInterval;
  private static int DEFAULT_REGISTRY_UPDATE_INTERVAL = 120;

  public ExecutorImpl(ExecutorContext executorContext, Dispatcher dispatcher) {
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
    this.executorContext = executorContext;
    this.dispatcher = dispatcher;
    this.stateMachine = stateMachineFactory.make(this);
    this.workerContext = null;
    this.exitCode = 0;
    this.failMsg = "";

    if (executorContext.getPrimusConf().getStreamingMode() == StreamingMode.PUSH) {
      needMoreTask = true;
    } else {
      needMoreTask = false;
    }
  }

  private static StateMachineFactory<ExecutorImpl, ExecutorState, ExecutorEventType,
      ExecutorEvent> stateMachineFactory =
      new StateMachineFactory<ExecutorImpl, ExecutorState, ExecutorEventType,
          ExecutorEvent>(ExecutorState.NEW)
          .addTransition(ExecutorState.NEW,
              ExecutorState.REGISTERED,
              ExecutorEventType.REGISTERED,
              new NewToRegisteredTransition())
          .addTransition(ExecutorState.REGISTERED,
              ExecutorState.STARTING,
              ExecutorEventType.START,
              new RegisteredToStartingTransition())
          .addTransition(ExecutorState.STARTING,
              ExecutorState.STARTING,
              ExecutorEventType.START)
          .addTransition(ExecutorState.STARTING,
              ExecutorState.RUNNING,
              ExecutorEventType.STARTED)
          .addTransition(ExecutorState.RUNNING,
              ExecutorState.RUNNING,
              ExecutorEventType.START)
          .addTransition(ExecutorState.REGISTERED,
              ExecutorState.EXITED_WITH_KILLED,
              ExecutorEventType.KILL,
              new KilledTransition())
          .addTransition(ExecutorState.STARTING,
              ExecutorState.KILLING,
              ExecutorEventType.KILL,
              new KillingTransition())
          .addTransition(ExecutorState.RUNNING,
              ExecutorState.KILLING,
              ExecutorEventType.KILL,
              new KillingTransition())
          .addTransition(ExecutorState.STARTING,
              EnumSet.of(ExecutorState.EXITED_WITH_FAILURE,
                  ExecutorState.RECOVERING),
              ExecutorEventType.FAILED,
              new FailedTransition())
          .addTransition(ExecutorState.RUNNING,
              EnumSet.of(ExecutorState.EXITED_WITH_FAILURE,
                  ExecutorState.RECOVERING),
              ExecutorEventType.FAILED,
              new FailedTransition())
          .addTransition(ExecutorState.KILLING,
              EnumSet.of(ExecutorState.EXITED_WITH_FAILURE,
                  ExecutorState.RECOVERING),
              ExecutorEventType.FAILED,
              new FailedTransition())
          .addTransition(ExecutorState.RECOVERING,
              ExecutorState.RUNNING,
              ExecutorEventType.STARTED)
          .addTransition(ExecutorState.RECOVERING,
              ExecutorState.KILLING,
              ExecutorEventType.KILL,
              new KillingTransition())
          .addTransition(ExecutorState.RUNNING,
              ExecutorState.EXITED_WITH_SUCCESS,
              ExecutorEventType.SUCCEEDED,
              new SucceededTransition())
          .addTransition(ExecutorState.RUNNING,
              ExecutorState.EXITED_WITH_KILLED,
              ExecutorEventType.KILLED,
              new KilledTransition())
          .addTransition(ExecutorState.KILLING,
              ExecutorState.KILLING,
              ExecutorEventType.START)
          .addTransition(ExecutorState.KILLING,
              ExecutorState.KILLING,
              ExecutorEventType.STARTED)
          .addTransition(ExecutorState.KILLING,
              ExecutorState.KILLING,
              ExecutorEventType.KILL)
          .addTransition(ExecutorState.KILLING,
              ExecutorState.EXITED_WITH_KILLED,
              ExecutorEventType.KILLED,
              new KilledTransition())
          .addTransition(ExecutorState.KILLING,
              ExecutorState.EXITED_WITH_SUCCESS,
              ExecutorEventType.SUCCEEDED,
              new SucceededTransition())
          .addTransition(ExecutorState.EXITED_WITH_SUCCESS,
              ExecutorState.EXITED_WITH_SUCCESS,
              ExecutorEventType.KILL)
          .addTransition(ExecutorState.EXITED_WITH_FAILURE,
              ExecutorState.EXITED_WITH_FAILURE,
              ExecutorEventType.KILL)
          .addTransition(ExecutorState.EXITED_WITH_KILLED,
              ExecutorState.EXITED_WITH_KILLED,
              ExecutorEventType.FAILED)
          .addTransition(ExecutorState.EXITED_WITH_KILLED,
              ExecutorState.EXITED_WITH_KILLED,
              ExecutorEventType.KILL)
          .installTopology();

  private final StateMachine<ExecutorState, ExecutorEventType, ExecutorEvent> stateMachine;

  @Override
  public ExecutorState getExecutorState() {
    return stateMachine.getCurrentState();
  }

  @Override
  public WorkerContext getWorkerContext() {
    return workerContext;
  }

  @Override
  public boolean getNeedMoreTask() {
    return needMoreTask;
  }

  @Override
  public void setNeedMoreTask(boolean needMoreTask) {
    this.needMoreTask = needMoreTask;
  }

  @Override
  public ExecutorStatus cloneAndGetExecutorStatus() {
    this.readLock.lock();
    try {
      return new ExecutorStatus(getExecutorState(), getNeedMoreTask(), exitCode, failMsg);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ExecutorId getExecutorId() {
    return executorContext.getExecutorId();
  }

  @Override
  public void handle(ExecutorEvent event) {
    try {
      this.writeLock.lock();
      ExecutorState oldState = stateMachine.getCurrentState();
      ExecutorState newState = null;
      try {
        newState =
            stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.warn("Can't handle this event at current state: Current: ["
            + oldState + "], eventType: [" + event.getType() + "]", e);
      }
      if (oldState != newState) {
        LOG.info("Executor " + executorContext.getExecutorId() + " transitioned from "
            + oldState
            + " to " + newState);
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  static class NewToRegisteredTransition implements SingleArcTransition<ExecutorImpl,
      ExecutorEvent> {

    @Override
    public void transition(ExecutorImpl executor, ExecutorEvent event) {
      ExecutorRegisteredEvent registeredEvent = (ExecutorRegisteredEvent) event;
      executor.workerContext = registeredEvent.getWorkerContext();
    }
  }

  static class RegisteredToStartingTransition implements SingleArcTransition<ExecutorImpl,
      ExecutorEvent> {

    @Override
    public void transition(ExecutorImpl executor, ExecutorEvent event) {
      LOG.info("Executor[" + executor.executorContext.getExecutorId() + "] begin to launch");
      Worker worker =
          new Worker(executor.executorContext, executor.workerContext, executor.dispatcher);
      executor.dispatcher.getEventHandler()
          .handle(new ChildLauncherEvent(ChildLauncherEventType.LAUNCH, worker));
    }
  }

  static class KillingTransition implements SingleArcTransition<ExecutorImpl, ExecutorEvent> {

    @Override
    public void transition(ExecutorImpl executor, ExecutorEvent event) {
      LOG.info("Executor[" + executor.executorContext.getExecutorId() + "] begin to stopProcess");
      executor.dispatcher.getEventHandler()
          .handle(new TaskRunnerManagerEvent(TaskRunnerManagerEventType.TASK_REMOVE_ALL,
              executor.getExecutorId()));
    }
  }

  static class KilledTransition implements SingleArcTransition<ExecutorImpl,
      ExecutorEvent> {

    @Override
    public void transition(ExecutorImpl executor, ExecutorEvent event) {
      executor.exitCode = ExecutorExitCode.KILLED.getValue();
      if (executor.executorContext.getChildLauncher().isExpectedExit()) {
        executor.failMsg = "killed by scheduler";
      } else {
        executor.failMsg = "killed by external signal (not send from AM )";
      }
    }
  }

  static class SucceededTransition implements SingleArcTransition<ExecutorImpl,
      ExecutorEvent> {

    @Override
    public void transition(ExecutorImpl executor, ExecutorEvent event) {
      LOG.info("Executor[" + executor.executorContext.getExecutorId() + "] begin to stopProcess");
      executor.exitCode = 0;
      executor.failMsg = "";
    }
  }

  static class FailedTransition implements MultipleArcTransition<ExecutorImpl,
      ExecutorEvent, ExecutorState> {

    @Override
    public ExecutorState transition(ExecutorImpl executor, ExecutorEvent event) {
      if (executor.workerContext.needRestart()) {
        LOG.info("Executor[" + executor.executorContext.getExecutorId() +
            "] worker failed, begin to recover");
        Worker worker = new Worker(executor.executorContext, executor.workerContext,
            executor.dispatcher);
        executor.dispatcher.getEventHandler()
            .handle(new ChildLauncherEvent(ChildLauncherEventType.RESTART, worker));
        executor.workerContext.increaseRestartTimes();
        return ExecutorState.RECOVERING;
      } else {
        ExecutorFailedEvent failedEvent = (ExecutorFailedEvent) event;
        executor.exitCode = failedEvent.getExitCode();
        executor.failMsg = failedEvent.getExitMessage();
        LOG.info("Executor[" + executor.executorContext.getExecutorId() + "] worker failed, exit:"
            + failedEvent.getExitCode() + ", msg:" + failedEvent.getExitMessage());
        return ExecutorState.EXITED_WITH_FAILURE;
      }
    }
  }
}
