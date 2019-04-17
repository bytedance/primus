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

package com.bytedance.primus.executor.consul.model;

import java.net.ServerSocket;
import java.util.Map;

public class ConsulEndpoint {
  private String serviceName;
  private String address;
  private ServerSocket serverSocket;
  private Map<String, String> tags;
  private int ttl;

  public ConsulEndpoint(String serviceName, String address, ServerSocket serverSocket,
      Map<String, String> tags, int ttl) {
    this.serviceName = serviceName;
    this.address = address;
    this.serverSocket = serverSocket;
    this.tags = tags;
    this.ttl = ttl;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public ServerSocket getServerSocket() {
    return serverSocket;
  }

  public void setServerSocket(ServerSocket serverSocket) {
    this.serverSocket = serverSocket;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public int getTtl() {
    return ttl;
  }

  public void setTtl(int ttl) {
    this.ttl = ttl;
  }

  @Override
  public String toString() {
    return "ConsulEndpoint{" +
        "serviceName='" + serviceName + '\'' +
        ", address='" + address + '\'' +
        ", serverSocket=" + serverSocket +
        ", tags=" + tags +
        ", ttl=" + ttl +
        '}';
  }
}
