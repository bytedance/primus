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

package com.bytedance.primus.am.apiserver;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.apiserver.client.apis.CoreApi;
import com.bytedance.primus.apiserver.client.apis.watch.ResourceEventHandler;
import com.bytedance.primus.apiserver.client.apis.watch.Watch;
import com.bytedance.primus.apiserver.client.models.Job;
import com.bytedance.primus.apiserver.records.Condition;
import com.bytedance.primus.apiserver.records.JobStatus;
import com.bytedance.primus.apiserver.records.RoleStatus;
import com.bytedance.primus.apiserver.records.impl.ConditionImpl;
import com.bytedance.primus.apiserver.records.impl.JobStatusImpl;
import com.bytedance.primus.apiserver.records.impl.RoleStatusImpl;
import com.bytedance.primus.common.service.AbstractService;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobController extends AbstractService {

  private static final Logger logger = LoggerFactory.getLogger(JobController.class);
  private static final int RECONCILE_INTERVAL = 20 * 1000;

  private AMContext context;
  private CoreApi coreApi;
  private Watch<Job> jobWatch;

  private Thread reconcileThread;
  private volatile boolean isStopped;
  private volatile String jobName;

  public JobController(AMContext context) {
    super(JobController.class.getName());
    this.context = context;
    coreApi = context.getCoreApi();
    isStopped = false;
    jobName = null;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    jobWatch = coreApi.createWatchList(Job.class, new JobEventHandler());
    reconcileThread = new JobReconcileThread();
    reconcileThread.setDaemon(true);
    reconcileThread.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    isStopped = true;
    jobWatch.cancel();
    reconcileThread.interrupt();
  }

  public Job getJob() {
    Job job = null;
    if (jobName != null) {
      try {
        job = coreApi.getJob(jobName);
      } catch (Exception e) {
        logger.warn("Failed to get job from api server", e);
      }
    }
    return job;
  }

  private void updateRoleStatuses(
      Iterable<String> roleNames,
      Map<String, RoleStatus> roleStatuses
  ) {
    for (String roleName : roleNames) {
      int priority = context
          .getRoleInfoManager()
          .getRoleInfo(roleName)
          .getPriority();

      RoleStatus roleStatus = new RoleStatusImpl();
      roleStatus.setActiveNum(context.getSchedulerExecutorManager().getRegisteredNum(priority));
      roleStatus.setSucceedNum(context.getSchedulerExecutorManager().getSucceedNum(priority));
      roleStatus.setFailedNum(context.getSchedulerExecutorManager().getFailedNum(priority));
      roleStatuses.put(roleName, roleStatus);
    }
  }

  private void updateJobConditions(JobStatus jobStatus, String type, String reason, String msg) {
    Condition condition = new ConditionImpl()
        .setType(type)
        .setReason(reason)
        .setMessage(msg)
        .setLastTransitionTime(System.currentTimeMillis())
        .setLastUpdateTime(System.currentTimeMillis());
    jobStatus.getConditions().add(condition);
  }

  private void updateJobToApiServer(Job job) {
    try {
      Job newJob = coreApi.replaceJob(job, job.getMeta().getVersion());
      logger.info("Job replaced\n{}", newJob);
    } catch (Exception e) {
      logger.warn("Failed to replace job " + job.getMeta().getName(), e);
    }
  }

  class JobReconcileThread extends Thread {

    public JobReconcileThread() {
      super(JobReconcileThread.class.getName());
    }

    @Override
    public void run() {
      while (!isStopped) {
        try {
          Thread.sleep(RECONCILE_INTERVAL);
        } catch (InterruptedException e) {
          // ignore
        }
        if (jobName == null) {
          continue;
        }
        try {
          Job job = coreApi.getJob(jobName);
          JobStatus newJobStatus = new JobStatusImpl(job.getStatus().getProto());
          updateRoleStatuses(job.getSpec().getRoleSpecs().keySet(), newJobStatus.getRoleStatuses());
          // Only update the difference to api server
          if (!newJobStatus.equals(job.getStatus())) {
            job.setStatus(newJobStatus);
            updateJobToApiServer(job);
          }
        } catch (Exception e) {
          logger.warn("Failed to reconcile job", e);
        }
      }
    }
  }

  class JobEventHandler implements ResourceEventHandler<Job> {

    @Override
    public void onAdd(Job job) {
      logger.info("Job added\n{}", job.toString());
      JobController.this.jobName = job.getMeta().getName();
      context.emitRoleInfoCreatedEvent(job.getSpec().getRoleSpecs());
      job.getStatus().setStartTime(System.currentTimeMillis());
      updateJobToApiServer(job);
    }

    @Override
    public void onUpdate(Job oldJob, Job newJob) {
      logger.info("Job updated\n{}\n{}", oldJob, newJob);
      if (oldJob.getSpec().equals(newJob.getSpec())) {
        logger.info("Job spec not changed");
        return;
      }

      logger.info("Job spec changed");
      context.emitRoleInfoUpdatedEvent(newJob.getSpec().getRoleSpecs());
      updateJobToApiServer(newJob);
    }

    @Override
    public void onDelete(Job job) {

    }

    @Override
    public void onError(Throwable throwable) {
      logger.error(JobEventHandler.class.getName() + " catches error", throwable);
    }
  }
}