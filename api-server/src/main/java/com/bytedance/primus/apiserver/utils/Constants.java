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

package com.bytedance.primus.apiserver.utils;

public class Constants {

  public static final String KIND_KEY = "kind";
  public static final String META_KEY = "meta";
  public static final String SPEC_KEY = "spec";
  public static final String STATUS_KEY = "status";

  public static final String API_SERVER_RPC_HOST_ENV = "API_SERVER_HOST";
  public static final String API_SERVER_RPC_PORT_ENV = "API_SERVER_PORT";
  public static final String PRIMUS_EXECUTOR_UNIQID_ENV = "PRIMUS_EXECUTOR_UNIQID";

  public static final int MAX_MESSAGE_SIZE = 1024 * 1024 * 128;
}
