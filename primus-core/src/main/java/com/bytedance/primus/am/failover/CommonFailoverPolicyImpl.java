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

package com.bytedance.primus.am.failover;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.apiserver.proto.UtilsProto.CommonFailoverPolicy;
import java.util.HashMap;
import java.util.Map;

public class CommonFailoverPolicyImpl implements FailoverPolicy {

  private AMContext context;
  private CommonFailoverPolicy failoverPolicy;
  private Map<ExecutorId, Integer> executorIdFailoverTimesMap;

  public CommonFailoverPolicyImpl(AMContext context, CommonFailoverPolicy failoverPolicy) {
    this.context = context;
    this.failoverPolicy = failoverPolicy;
    this.executorIdFailoverTimesMap = new HashMap<>();
  }

  @Override
  public boolean needFailover(SchedulerExecutor schedulerExecutor, boolean isFailed) {
    ExecutorId executorId = schedulerExecutor.getExecutorId();
    int times = executorIdFailoverTimesMap.getOrDefault(executorId, 0);
    boolean needFailover;
    switch (failoverPolicy.getRestartType()) {
      case ALWAYS:
        needFailover = true;
        break;
      case NEVER:
        needFailover = false;
        break;
      case ON_FAILURE:
        needFailover = isFailed;
        break;
      default:
        needFailover = false;
        break;
    }
    if (times >= failoverPolicy.getMaxFailureTimes() && needFailover) {
      String diag = "Executor " + executorId + " has been failed for "
          + times + " times, exceeding max failure times " + failoverPolicy.getMaxFailureTimes()
          + ", containerId " + schedulerExecutor.getContainer().getId() + ", nodeHttpAddress "
          + schedulerExecutor.getContainer().getNodeHttpAddress();
      int exitCode = ApplicationExitCode.FAILOVER.getValue();
      if (schedulerExecutor.getExecutorExitCode() == -104) {
        exitCode = ApplicationExitCode.EXECUTOR_OOM.getValue();
      } else if (schedulerExecutor.getExecutorExitCode() == 73) {
        exitCode = ApplicationExitCode.GANG_POLICY_FAILED.getValue();
      }
      switch (failoverPolicy.getMaxFailurePolicy()) {
        case FAIL_ATTEMPT:
          context.emitFailAttemptEvent(diag, exitCode);
          break;
        case FAIL_APP:
          context.emitFailApplicationEvent(diag, exitCode);
          break;
        case NONE:
          // do nothing for app, and executor itself do not need failover
          break;
      }
      return false;
    }
    return needFailover;
  }

  @Override
  public void increaseFailoverTimes(SchedulerExecutor schedulerExecutor) {
    ExecutorId executorId = schedulerExecutor.getExecutorId();
    int times = executorIdFailoverTimesMap.getOrDefault(executorId, 0);
    ++times;
    executorIdFailoverTimesMap.put(executorId, times);
  }
}
