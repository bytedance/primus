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

package com.bytedance.primus.utils;

import com.bytedance.primus.common.model.records.FinalApplicationStatus;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class AMProcessExitCodeHelper {

  private final FinalApplicationStatus finalApplicationStatus;

  private final Map<FinalApplicationStatus, Integer> exitMap = ImmutableMap.of(
      FinalApplicationStatus.SUCCEEDED, 0,
      FinalApplicationStatus.UNDEFINED, -1,
      FinalApplicationStatus.FAILED, -2,
      FinalApplicationStatus.KILLED, -3);

  public AMProcessExitCodeHelper(FinalApplicationStatus finalApplicationStatus) {
    this.finalApplicationStatus = finalApplicationStatus;
  }

  public int getExitCode() {
    return exitMap.getOrDefault(
        finalApplicationStatus,
        -1 /* Defaults to undefined */
    );
  }
}
