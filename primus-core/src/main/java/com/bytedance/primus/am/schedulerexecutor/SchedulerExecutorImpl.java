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

package com.bytedance.primus.am.schedulerexecutor;

import static com.bytedance.primus.executor.ExecutorExitCode.BLACKLISTED;
import static com.bytedance.primus.executor.ExecutorExitCode.EXPIRED;
import static com.bytedance.primus.executor.ExecutorExitCode.KILLED;
import static com.bytedance.primus.utils.PrimusConstants.BLACKLISTED_EXIT_MSG;
import static com.bytedance.primus.utils.PrimusConstants.EXPIRED_EXIT_MSG;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.container.ContainerManagerEvent;
import com.bytedance.primus.am.container.ContainerManagerEventType;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.ExecutorSpec;
import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.state.InvalidStateTransitonException;
import com.bytedance.primus.common.state.SingleArcTransition;
import com.bytedance.primus.common.state.StateMachine;
import com.bytedance.primus.common.state.StateMachineFactory;
import java.util.Date;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerExecutorImpl implements SchedulerExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulerExecutorImpl.class);

  private final AMContext context;
  private ExecutorId executorId;
  private ExecutorSpec spec;
  private final Object lock;
  private int containerExitCode;
  private String containerExitMsg;
  private int workerExitCode;
  private String workerExitMsg;
  private Container container;
  private Date launchTime = new Date();
  private Date releaseTime;

  public static final int UNINITIALIZED_EXIT_CODE = -1000;

  private static StateMachineFactory<SchedulerExecutorImpl, SchedulerExecutorState,
      SchedulerExecutorEventType, SchedulerExecutorEvent> stateMachineFactory =
      new StateMachineFactory<SchedulerExecutorImpl, SchedulerExecutorState,
          SchedulerExecutorEventType, SchedulerExecutorEvent>(SchedulerExecutorState.NEW)
          .addTransition(SchedulerExecutorState.NEW,
              SchedulerExecutorState.STARTING,
              SchedulerExecutorEventType.REGISTERED)
          .addTransition(SchedulerExecutorState.STARTING,
              SchedulerExecutorState.RUNNING,
              SchedulerExecutorEventType.STARTED)
          .addTransition(SchedulerExecutorState.RUNNING,
              SchedulerExecutorState.RUNNING,
              SchedulerExecutorEventType.STARTED)
          .addTransition(SchedulerExecutorState.STARTING,
              SchedulerExecutorState.FAILED,
              SchedulerExecutorEventType.FAILED,
              new CompletedTransition())
          .addTransition(SchedulerExecutorState.RUNNING,
              SchedulerExecutorState.FAILED,
              SchedulerExecutorEventType.FAILED,
              new CompletedTransition())
          .addTransition(SchedulerExecutorState.STARTING,
              SchedulerExecutorState.COMPLETED,
              SchedulerExecutorEventType.SUCCEEDED,
              new CompletedTransition())
          .addTransition(SchedulerExecutorState.RUNNING,
              SchedulerExecutorState.COMPLETED,
              SchedulerExecutorEventType.SUCCEEDED,
              new CompletedTransition())
          .addTransition(SchedulerExecutorState.KILLING,
              SchedulerExecutorState.COMPLETED,
              SchedulerExecutorEventType.SUCCEEDED,
              new CompletedTransition())

          .addTransition(SchedulerExecutorState.NEW,
              SchedulerExecutorState.EXPIRED,
              SchedulerExecutorEventType.EXPIRED,
              new ExpiredTransition())
          .addTransition(SchedulerExecutorState.STARTING,
              SchedulerExecutorState.EXPIRED,
              SchedulerExecutorEventType.EXPIRED,
              new ExpiredTransition())
          .addTransition(SchedulerExecutorState.RUNNING,
              SchedulerExecutorState.EXPIRED,
              SchedulerExecutorEventType.EXPIRED,
              new ExpiredTransition())
          .addTransition(SchedulerExecutorState.FAILED,
              SchedulerExecutorState.EXPIRED,
              SchedulerExecutorEventType.EXPIRED,
              new ExpiredTransition())
          .addTransition(SchedulerExecutorState.COMPLETED,
              SchedulerExecutorState.EXPIRED,
              SchedulerExecutorEventType.EXPIRED,
              new ExpiredTransition())
          .addTransition(SchedulerExecutorState.RELEASED,
              SchedulerExecutorState.RELEASED,
              SchedulerExecutorEventType.EXPIRED)

          .addTransition(SchedulerExecutorState.NEW,
              SchedulerExecutorState.KILLING,
              SchedulerExecutorEventType.BLACKLIST,
              new BlacklistedTransition())
          .addTransition(SchedulerExecutorState.STARTING,
              SchedulerExecutorState.KILLING,
              SchedulerExecutorEventType.BLACKLIST,
              new BlacklistedTransition())
          .addTransition(SchedulerExecutorState.RUNNING,
              SchedulerExecutorState.KILLING,
              SchedulerExecutorEventType.BLACKLIST,
              new BlacklistedTransition())
          .addTransition(SchedulerExecutorState.FAILED,
              SchedulerExecutorState.FAILED,
              SchedulerExecutorEventType.BLACKLIST)
          .addTransition(SchedulerExecutorState.COMPLETED,
              SchedulerExecutorState.COMPLETED,
              SchedulerExecutorEventType.BLACKLIST)
          .addTransition(SchedulerExecutorState.RELEASED,
              SchedulerExecutorState.RELEASED,
              SchedulerExecutorEventType.BLACKLIST)
          .addTransition(SchedulerExecutorState.KILLING,
              SchedulerExecutorState.KILLING,
              SchedulerExecutorEventType.BLACKLIST)
          .addTransition(SchedulerExecutorState.KILLED,
              SchedulerExecutorState.KILLED,
              SchedulerExecutorEventType.BLACKLIST)

          .addTransition(SchedulerExecutorState.NEW,
              SchedulerExecutorState.KILLING,
              SchedulerExecutorEventType.KILL)
          .addTransition(SchedulerExecutorState.STARTING,
              SchedulerExecutorState.KILLING,
              SchedulerExecutorEventType.KILL)
          .addTransition(SchedulerExecutorState.RUNNING,
              SchedulerExecutorState.KILLING,
              SchedulerExecutorEventType.KILL)
          .addTransition(SchedulerExecutorState.FAILED,
              SchedulerExecutorState.FAILED,
              SchedulerExecutorEventType.KILL)
          .addTransition(SchedulerExecutorState.COMPLETED,
              SchedulerExecutorState.COMPLETED,
              SchedulerExecutorEventType.KILL)
          .addTransition(SchedulerExecutorState.RELEASED,
              SchedulerExecutorState.RELEASED,
              SchedulerExecutorEventType.KILL)

          // TODO: Streamline core state machines
          .addTransition(SchedulerExecutorState.NEW,
              SchedulerExecutorState.KILLING_FORCIBLY,
              SchedulerExecutorEventType.KILL_FORCIBLY)
          .addTransition(SchedulerExecutorState.STARTING,
              SchedulerExecutorState.KILLING_FORCIBLY,
              SchedulerExecutorEventType.KILL_FORCIBLY)
          .addTransition(SchedulerExecutorState.RUNNING,
              SchedulerExecutorState.KILLING_FORCIBLY,
              SchedulerExecutorEventType.KILL_FORCIBLY)
          .addTransition(SchedulerExecutorState.FAILED,
              SchedulerExecutorState.FAILED,
              SchedulerExecutorEventType.KILL_FORCIBLY)
          .addTransition(SchedulerExecutorState.COMPLETED,
              SchedulerExecutorState.COMPLETED,
              SchedulerExecutorEventType.KILL_FORCIBLY)
          .addTransition(SchedulerExecutorState.RELEASED,
              SchedulerExecutorState.RELEASED,
              SchedulerExecutorEventType.KILL_FORCIBLY)

          .addTransition(SchedulerExecutorState.KILLING,
              SchedulerExecutorState.KILLING,
              SchedulerExecutorEventType.REGISTERED)
          .addTransition(SchedulerExecutorState.KILLING,
              SchedulerExecutorState.KILLING,
              SchedulerExecutorEventType.KILL)
          .addTransition(SchedulerExecutorState.RUNNING,
              SchedulerExecutorState.KILLED,
              SchedulerExecutorEventType.KILLED,
              new KilledTransition())
          .addTransition(SchedulerExecutorState.KILLING,
              SchedulerExecutorState.KILLED,
              SchedulerExecutorEventType.KILLED,
              new KilledTransition())
          .addTransition(SchedulerExecutorState.KILLING_FORCIBLY,
              SchedulerExecutorState.KILLED,
              SchedulerExecutorEventType.KILLED,
              new KilledTransition())
          .addTransition(SchedulerExecutorState.KILLING,
              SchedulerExecutorState.RELEASED,
              SchedulerExecutorEventType.RELEASED)

          .addTransition(SchedulerExecutorState.NEW,
              SchedulerExecutorState.RELEASED,
              SchedulerExecutorEventType.RELEASED,
              new ReleasedTransition())
          .addTransition(SchedulerExecutorState.STARTING,
              SchedulerExecutorState.RELEASED,
              SchedulerExecutorEventType.RELEASED,
              new ReleasedTransition())
          .addTransition(SchedulerExecutorState.RUNNING,
              SchedulerExecutorState.RELEASED,
              SchedulerExecutorEventType.RELEASED,
              new ReleasedTransition())
          .addTransition(SchedulerExecutorState.FAILED,
              SchedulerExecutorState.RELEASED,
              SchedulerExecutorEventType.RELEASED,
              new ReleasedTransition())
          .addTransition(SchedulerExecutorState.COMPLETED,
              SchedulerExecutorState.RELEASED,
              SchedulerExecutorEventType.RELEASED,
              new ReleasedTransition())
          .addTransition(SchedulerExecutorState.EXPIRED,
              SchedulerExecutorState.RELEASED,
              SchedulerExecutorEventType.RELEASED,
              new ReleasedTransition())
          .installTopology();

  private final StateMachine<SchedulerExecutorState, SchedulerExecutorEventType,
      SchedulerExecutorEvent> stateMachine;

  public SchedulerExecutorImpl(AMContext context, ExecutorId executorId, Container container) {
    this.context = context;
    this.executorId = executorId;
    this.stateMachine = stateMachineFactory.make(this);
    this.spec = null;
    this.container = container;
    this.workerExitCode = UNINITIALIZED_EXIT_CODE;
    this.workerExitMsg = "";
    this.containerExitCode = UNINITIALIZED_EXIT_CODE;
    this.containerExitMsg = "";
    lock = new Object();
  }

  @Override
  public SchedulerExecutorState getExecutorState() {
    return stateMachine.getCurrentState();
  }

  @Override
  public ExecutorSpec getSpec() {
    return spec;
  }

  @Override
  public void setSpec(ExecutorSpec spec) {
    this.spec = spec;
  }

  @Override
  public ExecutorId getExecutorId() {
    return executorId;
  }

  @Override
  public void setExecutorId(ExecutorId executorId) {
    this.executorId = executorId;
  }

  @Override
  public Container getContainer() {
    return container;
  }

  @Override
  public int getPriority() {
    return container.getPriority().getPriority();
  }

  @Override
  public int getContainerExitCode() {
    return containerExitCode;
  }

  @Override
  public String getContainerExitMsg() {
    return containerExitMsg;
  }

  @Override
  public int getWorkerExitCode() {
    return workerExitCode;
  }

  @Override
  public String getWorkerExitMsg() {
    return workerExitMsg;
  }

  @Override
  public int getExecutorExitCode() {
    if (workerExitCode != SchedulerExecutorImpl.UNINITIALIZED_EXIT_CODE) {
      return workerExitCode;
    } else {
      return containerExitCode;
    }
  }

  @Override
  public String getExecutorExitMsg() {
    String diag = "";
    if (!workerExitMsg.isEmpty()) {
      diag = workerExitMsg;
    } else if (!containerExitMsg.isEmpty()) {
      diag = containerExitMsg;
    }
    return diag;
  }

  @Override
  public boolean isSuccess() {
    if (workerExitCode != UNINITIALIZED_EXIT_CODE) {
      if (workerExitCode == KILLED.getValue()) {
        if (containerExitCode == BLACKLISTED.getValue()) {
          return false;
        }
        return true;
      } else {
        return workerExitCode == 0;
      }
    } else {
      return containerExitCode == 0;
    }
  }

  @Override
  public Date getLaunchTime() {
    return launchTime;
  }

  @Override
  public Date getReleaseTime() {
    return releaseTime;
  }

  @Override
  public void handle(SchedulerExecutorEvent event) {
    synchronized (lock) {
      SchedulerExecutorState oldState = stateMachine.getCurrentState();
      SchedulerExecutorState newState = null;
      try {
        newState = stateMachine.doTransition(event.getType(), event);
        if (newState != oldState) {
          context.logStatusEvent(context.getStatusEventWrapper().buildWorkerStatusEvent(this));
        }
      } catch (InvalidStateTransitonException e) {
        LOG.warn("Can't handle this event at current state: Current: ["
            + oldState + "], eventType: [" + event.getType() + "]");
      }
      if (oldState != newState) {
        LOG.info("Executor " + executorId + " transitioned from "
            + oldState + " to " + newState);

        SchedulerExecutorStateChangeEvent executorStateChangeEvent = new SchedulerExecutorStateChangeEvent(
            SchedulerExecutorStateChangeEventType.STATE_CHANGED, this);
        LOG.debug("Release STATE_CHANGED event:" + executorStateChangeEvent);
        context.getDispatcher().getEventHandler().handle(executorStateChangeEvent);
      }
    }
  }

  static class CompletedTransition implements SingleArcTransition<SchedulerExecutorImpl,
      SchedulerExecutorEvent> {

    @Override
    public void transition(SchedulerExecutorImpl executor, SchedulerExecutorEvent e) {
      SchedulerExecutorCompletedEvent event = (SchedulerExecutorCompletedEvent) e;
      executor.workerExitCode = event.getExitCode();
      executor.workerExitMsg = event.getExitMsg();
    }
  }

  static class ExpiredTransition implements SingleArcTransition<SchedulerExecutorImpl,
      SchedulerExecutorEvent> {

    @Override
    public void transition(SchedulerExecutorImpl executor, SchedulerExecutorEvent e) {
      executor.workerExitCode = EXPIRED.getValue();
      executor.workerExitMsg = EXPIRED_EXIT_MSG;
      executor.context.getDispatcher().getEventHandler().handle(
          new ContainerManagerEvent(ContainerManagerEventType.EXECUTOR_EXPIRED, executor.container)
      );
    }
  }

  static class BlacklistedTransition implements SingleArcTransition<SchedulerExecutorImpl,
      SchedulerExecutorEvent> {

    @Override
    public void transition(SchedulerExecutorImpl executor, SchedulerExecutorEvent e) {
      executor.containerExitCode = BLACKLISTED.getValue();
      executor.containerExitMsg = BLACKLISTED_EXIT_MSG;
    }
  }

  static class ReleasedTransition implements SingleArcTransition<SchedulerExecutorImpl,
      SchedulerExecutorEvent> {

    @Override
    public void transition(SchedulerExecutorImpl executor, SchedulerExecutorEvent e) {
      SchedulerExecutorReleasedEvent event = (SchedulerExecutorReleasedEvent) e;
      executor.containerExitCode = event.getExitCode();
      executor.containerExitMsg = event.getExitMsg();
      executor.releaseTime = new Date();
    }
  }

  static class KilledTransition implements SingleArcTransition<SchedulerExecutorImpl,
      SchedulerExecutorEvent> {

    @Override
    public void transition(SchedulerExecutorImpl executor, SchedulerExecutorEvent e) {
      SchedulerExecutorCompletedEvent event = (SchedulerExecutorCompletedEvent) e;
      executor.workerExitCode = event.getExitCode();
      executor.workerExitMsg = event.getExitMsg();
      executor.releaseTime = new Date();
      executor.context.getDispatcher().getEventHandler().handle(
          new SchedulerExecutorManagerContainerCompletedEvent(
              SchedulerExecutorManagerEventType.CONTAINER_KILLED,
              executor.container, event.getExitCode(), event.getExitMsg())
      );
    }
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", SchedulerExecutorImpl.class.getSimpleName() + "[", "]")
        .add("executorId=" + executorId)
        .add("stateMachine=" + stateMachine.getCurrentState())
        .toString();
  }
}
