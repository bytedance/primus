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

package com.bytedance.primus.apiserver.client;

import com.bytedance.primus.apiserver.proto.ResourceServiceProto.ReadOptions;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchListRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchListResponse;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchRequest;
import com.bytedance.primus.apiserver.proto.ResourceServiceProto.WatchResponse;
import com.bytedance.primus.apiserver.records.Resource;
import com.bytedance.primus.apiserver.service.exception.ApiServerException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;

public interface Client {

  Resource create(Resource resource) throws ApiServerException;

  void delete(String resourceKind, String resourceName)
      throws ApiServerException;

  void deleteList(String resourceKind) throws ApiServerException;

  Resource replace(Resource resource, long resourceVersion) throws ApiServerException;

  Resource patch(Resource resource, long resourceVersion) throws ApiServerException;

  Resource get(String resourceKind, String resourceName, ReadOptions options)
      throws ApiServerException;

  List<Resource> list(String resourceKind, ReadOptions options) throws ApiServerException;

  List<Resource> listByFilter(String resourceKind, Map<String,String> filter, ReadOptions options)
      throws ApiServerException;

  List<Resource> listByFilterNot(String resourceKind, Map<String,String> filter, ReadOptions options)
      throws ApiServerException;

  StreamObserver<WatchRequest> watch(StreamObserver<WatchResponse> streamObserver)
      throws ApiServerException;

  StreamObserver<WatchListRequest> watchList(StreamObserver<WatchListResponse> streamObserver)
      throws ApiServerException;

  Resource getStatus(String resourceKind, String resourceName, ReadOptions options)
      throws ApiServerException;

  Resource replaceStatus(Resource resource, long resourceVersion) throws ApiServerException;

  Resource patchStatus(Resource resource, long resourceVersion) throws ApiServerException;
}
