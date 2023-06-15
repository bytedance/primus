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

package com.bytedance.primus.apiserver.service.exception;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiServerException extends Exception {

  private static final Logger log = LoggerFactory.getLogger(ApiServerException.class);
  private ErrorCode errorCode;

  public ApiServerException(ErrorCode code, String message) {
    this(code, message, null);
  }

  public ApiServerException(StatusRuntimeException se) {
    super(se.getMessage(), se.getStatus().getCause());
    errorCode = ErrorCode.fromGrpcStatus(se.getStatus());
  }

  public ApiServerException(Throwable throwable) {
    this(throwable.getMessage(), throwable);
  }

  public ApiServerException(String message, Throwable throwable) {
    super(message, wrapThrowable(message, throwable));
    errorCode = ErrorCode.fromGrpcStatus(Status.fromThrowable(throwable));
  }

  public ApiServerException(ErrorCode errorCode, String message, Throwable throwable) {
    super(message, wrapThrowable(errorCode, message, throwable));
    this.errorCode = errorCode;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  private static Throwable wrapThrowable(String message, Throwable throwable) {
    if (throwable instanceof ApiServerException) {
      ApiServerException apiServerException = (ApiServerException) throwable;
      Throwable cause = apiServerException.getCause();
      if (cause instanceof StatusRuntimeException) {
        return ((StatusRuntimeException) cause).getStatus().withDescription(message).asException();
      } else {
        return wrapThrowable(ErrorCode.INTERNAL, message, cause);
      }
    }
    return wrapThrowable(ErrorCode.INTERNAL, message, throwable);
  }

  private static Throwable wrapThrowable(ErrorCode errorCode, String message, Throwable throwable) {
    try {
      if (StringUtils.isEmpty(message)) {
        message =
            throwable == null
                ? "Unknown Exception，Please checkout Am logs"
                : throwable.getMessage();
      }
      Status status = Status.fromCodeValue(errorCode.getCode()).withDescription(message);
      if (throwable != null) {
        return new StatusRuntimeException(status.withCause(throwable));
      }
      return new StatusRuntimeException(status);
    } catch (Throwable t) {
      log.error("Failed to wrapThrowable", t);
      return Status.INTERNAL.withCause(throwable).asException();
    }
  }
}
