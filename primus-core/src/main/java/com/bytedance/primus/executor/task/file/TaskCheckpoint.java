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

package com.bytedance.primus.executor.task.file;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class TaskCheckpoint {

  private static final String POS_KEY = "pos";
  private static final String ACCURATE_KEY = "accurate";

  private long pos;
  private boolean accurate;  // whether the pos is accurate

  public TaskCheckpoint() {
    pos = 0;
    accurate = false;
  }

  public void parseCheckpoint(String checkpointStr) throws JSONException {
    if (!checkpointStr.isEmpty()) {
      JSONObject json = new JSONObject(checkpointStr);
      pos = json.getLong(POS_KEY);
      accurate = json.getBoolean(ACCURATE_KEY);
    }
  }

  public String getCheckpoint() {
    try {
      JSONObject jo = new JSONObject();
      jo.put(POS_KEY, pos);
      jo.put(ACCURATE_KEY, accurate);
      return jo.toString();
    } catch (JSONException e) {
      return "";
    }
  }

  public long getPos() {
    return pos;
  }

  public void setPos(long pos) {
    this.pos = pos;
  }

  public boolean getAccurate() {
    return accurate;
  }

  public void setAccurate(boolean accurate) {
    this.accurate = accurate;
  }
}
