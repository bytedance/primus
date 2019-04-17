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

package com.bytedance.primus.apiserver.records.impl;

import com.bytedance.primus.apiserver.proto.ResourceProto;
import com.bytedance.primus.apiserver.records.ExecutorStatus;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ExecutorStatusImpl implements ExecutorStatus {

  private ResourceProto.ExecutorStatus proto = ResourceProto.ExecutorStatus.getDefaultInstance();
  private ResourceProto.ExecutorStatus.Builder builder = null;
  private boolean viaProto = false;

  private List<String> networkSockets;
  private String hostname;
  private Map<String, String> metrics;

  public ExecutorStatusImpl() {
    builder = ResourceProto.ExecutorStatus.newBuilder();
  }

  public ExecutorStatusImpl(ResourceProto.ExecutorStatus proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized ExecutorStatus setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime(startTime);
    return this;
  }

  @Override
  public synchronized long getStartTime() {
    ResourceProto.ExecutorStatusOrBuilder p = viaProto ? proto : builder;
    return p.getStartTime();
  }

  @Override
  public synchronized ExecutorStatus setCompleteTime(long completeTime) {
    maybeInitBuilder();
    builder.setCompletionTime(completeTime);
    return this;
  }

  @Override
  public synchronized long getCompleteTime() {
    ResourceProto.ExecutorStatusOrBuilder p = viaProto ? proto : builder;
    return p.getCompletionTime();
  }

  @Override
  public synchronized ExecutorStatus setState(String state) {
    maybeInitBuilder();
    builder.setState(state);
    return this;
  }

  @Override
  public synchronized String getState() {
    ResourceProto.ExecutorStatusOrBuilder p = viaProto ? proto : builder;
    return p.getState();
  }

  @Override
  public synchronized ExecutorStatus setExitStatus(int exitStatus) {
    maybeInitBuilder();
    builder.setExitStatus(exitStatus);
    return this;
  }

  @Override
  public synchronized int getExitStatus() {
    ResourceProto.ExecutorStatusOrBuilder p = viaProto ? proto : builder;
    return p.getExitStatus();
  }

  @Override
  public synchronized ExecutorStatus setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    builder.setDiagnostics(diagnostics);
    return this;
  }

  @Override
  public synchronized String getDiagnostics() {
    ResourceProto.ExecutorStatusOrBuilder p = viaProto ? proto : builder;
    return p.getDiagnostics();
  }

  @Override
  public synchronized ExecutorStatus setNetworkSockets(List<String> networkSockets) {
    maybeInitBuilder();
    if (networkSockets == null) {
      builder.clearNetworkSockets();
    }
    this.networkSockets = networkSockets;
    return this;
  }

  @Override
  public synchronized List<String> getNetworkSockets() {
    if (networkSockets != null) {
      return networkSockets;
    }
    ResourceProto.ExecutorStatusOrBuilder p = viaProto ? proto : builder;
    networkSockets = new LinkedList<>(p.getNetworkSocketsList());
    return networkSockets;
  }

  @Override
  public ExecutorStatus setHostname(String hostname) {
    maybeInitBuilder();
    if (hostname == null) {
      builder.clearHostname();
      return this;
    }
    this.builder.setHostname(hostname);
    return this;
  }

  @Override
  public String getHostname() {
    if (hostname != null) {
      return hostname;
    }
    ResourceProto.ExecutorStatusOrBuilder p = viaProto ? proto : builder;
    return p.getHostname();
  }

  @Override
  public synchronized ExecutorStatus setMetrics(Map<String, String> metrics) {
    maybeInitBuilder();
    if (metrics == null) {
      builder.clearMetrics();
    }
    this.metrics = metrics;
    return this;
  }

  @Override
  public synchronized Map<String, String> getMetrics() {
    if (metrics != null) {
      return metrics;
    }
    ResourceProto.ExecutorStatusOrBuilder p = viaProto ? proto : builder;
    metrics = new HashMap<>(p.getMetricsMap());
    return metrics;
  }

  @Override
  public synchronized ResourceProto.ExecutorStatus getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof ExecutorStatusImpl)) {
      return false;
    }

    ExecutorStatusImpl other = (ExecutorStatusImpl) obj;
    boolean result = true;
    result = result && (getStartTime() == other.getStartTime());
    result = result && (getCompleteTime() == other.getCompleteTime());
    result = result && (getState().equals(other.getState()));
    result = result && (getExitStatus() == other.getExitStatus());
    result = result && (getDiagnostics().equals(other.getDiagnostics()));
    result = result && (getNetworkSockets().equals(other.getNetworkSockets()));
    result = result && (getMetrics().equals(other.getMetrics()));
    result = result && (getHostname().equals(other.getHostname()));
    return result;
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceProto.ExecutorStatus.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    if (networkSockets != null) {
      addNetworkSocketsToProto();
    }
    if (metrics != null) {
      addMetricsToProto();
    }
  }

  private synchronized void addNetworkSocketsToProto() {
    maybeInitBuilder();
    builder.clearNetworkSockets();
    if (networkSockets == null) {
      return;
    }
    builder.addAllNetworkSockets(networkSockets);
  }

  private synchronized void addMetricsToProto() {
    maybeInitBuilder();
    builder.clearMetrics();
    if (metrics == null) {
      return;
    }
    builder.putAllMetrics(metrics);
  }
}
