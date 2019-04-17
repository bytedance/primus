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

package com.bytedance.primus.am.eventlog;

import com.bytedance.primus.common.event.AbstractEvent;
import com.bytedance.primus.proto.EventLog.PrimusEventMsg;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusEvent<TYPE extends Enum<TYPE>> extends AbstractEvent<TYPE> {

  private static final Logger log = LoggerFactory.getLogger(StatusEvent.class);
  private PrimusEventMsg msg;

  public StatusEvent(TYPE type) {
    super(type);
  }

  public StatusEvent(TYPE type, PrimusEventMsg msg) {
    super(type);
    this.msg = msg;
  }

  public StatusEvent(TYPE type, long timestamp) {
    super(type, timestamp);
  }

  public PrimusEventMsg getMsg() {
    return msg;
  }

  public void setMsg(PrimusEventMsg msg) {
    this.msg = msg;
  }

  @Override
  public String toString() {
    try {
      return JsonFormat.printer().print(msg);
    } catch (Exception e) {
      log.error("Failed to jsonfy msg: {}", msg);
    }
    return null;
  }

  public byte[] toByteArray() {
    try {
      return msg.toByteArray();
    } catch (Exception e) {
      log.error("Failed to get byte[] msg: {}", msg);
    }
    return null;
  }
}
