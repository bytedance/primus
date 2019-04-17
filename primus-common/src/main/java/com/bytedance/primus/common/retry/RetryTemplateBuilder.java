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

public class RetryTemplateBuilder {

  private RetryPolicy baseRetryPolicy;
  private BackOffPolicy backOffPolicy;
  private BinaryExceptionClassifierBuilder classifierBuilder;

  public RetryTemplateBuilder maxAttempts(int maxAttempts) {
    this.baseRetryPolicy = new MaxAttemptsRetryPolicy(maxAttempts);
    return this;
  }

  public RetryTemplateBuilder backOffPeriod(long backoffPeriod) {
    this.backOffPolicy = new BackOffPolicy(backoffPeriod);
    return this;
  }

  public RetryTemplateBuilder retryOn(Class<? extends Throwable> throwable) {
    classifierBuilder().retryOn(throwable);
    return this;
  }

  private BinaryExceptionClassifierBuilder classifierBuilder() {
    if (this.classifierBuilder == null) {
      this.classifierBuilder = new BinaryExceptionClassifierBuilder();
    }
    return this.classifierBuilder;
  }

  public RetryTemplate build() {
    RetryTemplate retryTemplate = new RetryTemplate();
    if (this.baseRetryPolicy == null) {
      this.baseRetryPolicy = new MaxAttemptsRetryPolicy();
    }
    BinaryExceptionClassifier binaryExceptionClassifier = classifierBuilder().build();
    CompositeRetryPolicy finalPolicy = new CompositeRetryPolicy();
    BinaryExceptionClassifierRetryPolicy binaryExceptionClassifierRetryPolicy = new BinaryExceptionClassifierRetryPolicy(
        binaryExceptionClassifier);
    finalPolicy
        .setPolicies(new RetryPolicy[]{baseRetryPolicy, binaryExceptionClassifierRetryPolicy});
    retryTemplate.setRetryPolicy(finalPolicy);
    if (backOffPolicy != null) {
      retryTemplate.setBackOffPolicy(backOffPolicy);
    }
    return retryTemplate;
  }

}
