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

public class RetryTemplate {

  private RetryPolicy retryPolicy;

  private RetryContext retryContext;

  private BackOffPolicy backOffPolicy;

  public <T, E extends Throwable> T execute(RetryCallback<T, E> retryCallback) throws E {
    retryContext = new RetryContext();

    try {
      return retryCallback.doWithRetry(retryContext);
    } catch (Throwable ex) {
      retryContext.setLastThrowable(ex);
    }

    while (canRetry(retryPolicy, retryContext)) {
      try {
        retryContext.setLastThrowable(null);
        if (backOffPolicy != null) {
          backOffPolicy.backOff();
        }
        return retryCallback.doWithRetry(retryContext);
      } catch (Throwable e) {
        retryContext.setLastThrowable(e);
      }
    }
    throw RetryTemplate.<E>wrapIfNecessary(retryContext.getLastThrowable());
  }

  public static RetryTemplateBuilder builder() {
    return new RetryTemplateBuilder();
  }

  protected boolean canRetry(RetryPolicy retryPolicy, RetryContext context) {
    return retryPolicy.canRetry(context);
  }

  private static <E extends Throwable> E wrapIfNecessary(Throwable throwable)
      throws RetryFailedException {
    if (throwable instanceof Error) {
      throw (Error) throwable;
    } else if (throwable instanceof Exception) {
      @SuppressWarnings("unchecked")
      E rethrow = (E) throwable;
      return rethrow;
    } else {
      throw new RetryFailedException(throwable);
    }
  }

  public void setRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  public RetryContext getRetryContext() {
    return retryContext;
  }

  public BackOffPolicy getBackOffPolicy() {
    return backOffPolicy;
  }

  public void setBackOffPolicy(BackOffPolicy backOffPolicy) {
    this.backOffPolicy = backOffPolicy;
  }
}
