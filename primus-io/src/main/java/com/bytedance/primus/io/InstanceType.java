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

package com.bytedance.primus.io;

import java.util.HashMap;
import java.util.Map;

public enum InstanceType {
  INSTANCE_PROTO(0),
  EXAMPLE(2),
  ARROW(3);

  private int value;
  private static Map<Integer, InstanceType> map = new HashMap<>();

  InstanceType(int value) {
    this.value = value;
  }

  static {
    for (InstanceType instanceType : InstanceType.values()) {
      map.put(instanceType.value, instanceType);
    }
  }

  public int value() {
    return value;
  }

  public static InstanceType valueOf(int value) {
    return map.get(value);
  }
}
