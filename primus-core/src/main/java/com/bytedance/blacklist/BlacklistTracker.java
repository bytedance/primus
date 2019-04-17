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

package com.bytedance.blacklist;

import java.util.Map;

public interface BlacklistTracker {
  void addTaskFailure(String taskId, String containerId, String nodeId, long taskFailureTime);

  void addContainerFailure(String containerId, String nodeId, long containerFailureTime);

  boolean isContainerBlacklisted(String containerId);

  boolean isNodeBlacklisted(String nodeId);

  Map<String, Long> getNodeBlacklist();

  void addNodeBlackList(Map<String, Long> blackList);

  void removeNodeBlackList(Map<String, Long> blackList);
}
