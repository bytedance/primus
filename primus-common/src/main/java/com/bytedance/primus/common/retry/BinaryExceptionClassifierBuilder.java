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

package com.bytedance.primus.common.retry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinaryExceptionClassifierBuilder {

  private List<Class<? extends Throwable>> exceptionClasses = new ArrayList<Class<? extends Throwable>>();

  public void retryOn(Class<? extends Throwable> throwable) {
    exceptionClasses.add(throwable);
  }

  public BinaryExceptionClassifier build() {
    Map<Class<? extends Throwable>, Boolean> map = new HashMap<Class<? extends Throwable>, Boolean>();
    for (Class<? extends Throwable> type : exceptionClasses) {
      map.put(type, true);
    }
    BinaryExceptionClassifier binaryExceptionClassifier = new BinaryExceptionClassifier(map);
    return binaryExceptionClassifier;
  }


}
