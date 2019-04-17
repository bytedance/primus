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

package com.bytedance.primus.runtime.kubernetesnative.common.pods;

import com.bytedance.primus.proto.PrimusRuntime.RuntimeConf;
import com.bytedance.primus.runtime.kubernetesnative.common.KubernetesSchedulerConfig;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import java.util.Map;
import org.apache.hadoop.fs.Path;

public class PrimusPodContext {

  private String jobName;
  private String appName;
  private String kubernetesJobName;
  private String owner;
  private String psm;
  private Path hdfsStagingDir;
  private V1OwnerReference driverPodOwnerReference;
  private String driverPodUid;
  private Map<String, String> driverEnvironMap;
  private Map<String, String> jobEnvironMap;
  private int sleepSecondsBeforePodExit;

  private RuntimeConf runtimeConf;

  private KubernetesSchedulerConfig kubernetesSchedulerConfig;

  public RuntimeConf getRuntimeConf() {
    return runtimeConf;
  }

  public void setRuntimeConf(RuntimeConf runtimeConf) {
    this.runtimeConf = runtimeConf;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public String getPsm() {
    return psm;
  }

  public void setPsm(String psm) {
    this.psm = psm;
  }

  public Path getHdfsStagingDir() {
    return hdfsStagingDir;
  }

  public void setHdfsStagingDir(Path hdfsStagingDir) {
    this.hdfsStagingDir = hdfsStagingDir;
  }

  public void setOwnerReference(V1OwnerReference driverPodOwnerReference) {
    this.driverPodOwnerReference = driverPodOwnerReference;
  }

  public V1OwnerReference getDriverPodOwnerReference() {
    return driverPodOwnerReference;
  }

  public String getDriverPodUid() {
    return driverPodUid;
  }

  public void setDriverPodUid(String driverPodUid) {
    this.driverPodUid = driverPodUid;
  }

  public Map<String, String> getDriverEnvironMap() {
    return driverEnvironMap;
  }

  public void setDriverEnvironMap(Map<String, String> driverEnvironMap) {
    this.driverEnvironMap = driverEnvironMap;
  }

  public int getSleepSecondsBeforePodExit() {
    return sleepSecondsBeforePodExit;
  }

  public void setSleepSecondsBeforePodExit(int sleepSecondsBeforePodExit) {
    this.sleepSecondsBeforePodExit = sleepSecondsBeforePodExit;
  }

  public String getKubernetesJobName() {
    return kubernetesJobName;
  }

  public void setKubernetesJobName(String kubernetesJobName) {
    this.kubernetesJobName = kubernetesJobName;
  }

  public Map<String, String> getJobEnvironMap() {
    return jobEnvironMap;
  }

  public void setJobEnvironMap(Map<String, String> jobEnvironMap) {
    this.jobEnvironMap = jobEnvironMap;
  }

  public KubernetesSchedulerConfig getKubernetesSchedulerConfig() {
    return kubernetesSchedulerConfig;
  }

  public void setKubernetesSchedulerConfig(KubernetesSchedulerConfig kubernetesSchedulerConfig) {
    this.kubernetesSchedulerConfig = kubernetesSchedulerConfig;
  }
}
