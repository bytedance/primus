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

package com.bytedance.primus.am.progress;

import com.bytedance.primus.am.AMContext;
import java.util.Map;

public class RoleProgressManager extends ProgressManager {

  private final AMContext context;

  public RoleProgressManager(String name, AMContext context) {
    super(name);
    this.context = context;
  }

  @Override
  public void update() {
    int needFinishNum = 0;
    for (Map.Entry<Integer, Integer> entry :
        context.getSchedulerExecutorManager().getPriorityFinishNumMap().entrySet()) {
      needFinishNum += entry.getValue();
    }

    if (needFinishNum != 0) {
      int successNum = 0;
      Map<Integer, Integer> priorityFinishNumMap =
          context.getSchedulerExecutorManager().getPriorityFinishNumMap();
      for (Map.Entry<Integer, Integer> entry :
          context.getSchedulerExecutorManager().getPrioritySuccessNumMap().entrySet()) {
        int priority = entry.getKey();
        if (entry.getValue() > priorityFinishNumMap.get(priority)) {
          successNum += priorityFinishNumMap.get(priority);
        } else {
          successNum += entry.getValue();
        }
      }
      setProgress((float) successNum / needFinishNum);
    }
  }
}
