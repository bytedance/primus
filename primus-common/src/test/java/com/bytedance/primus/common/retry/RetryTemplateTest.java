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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class RetryTemplateTest {

  @Test
  public void testRetryAndThenSuccess() throws Throwable {
    RetryTemplate template = RetryTemplate.builder().maxAttempts(2)
        .retryOn(IOException.class)
        .build();
    AtomicInteger atomicInteger = new AtomicInteger(0);
    String result = template.execute(new RetryCallback<String, Throwable>() {
      @Override
      public String doWithRetry(RetryContext context) throws Throwable {
        if (atomicInteger.getAndIncrement() < 1) {
          throw new IOException("error");
        }
        return "SUCCESS" + "-" + atomicInteger.get();
      }
    });
    Assert.assertEquals(2, atomicInteger.get());
    Assert.assertEquals("SUCCESS-2", result);

  }

  @Test(expected = RuntimeException.class)
  public void testRetryAndThenFailed() throws Throwable {
    RetryTemplate template = RetryTemplate.builder().maxAttempts(1)
        .retryOn(IOException.class)
        .build();
    AtomicInteger atomicInteger = new AtomicInteger(0);
    String result = template.execute(new RetryCallback<String, Throwable>() {
      @Override
      public String doWithRetry(RetryContext context) throws Throwable {
        if (atomicInteger.getAndIncrement() <= 1) {
          throw new RuntimeException("error");
        }
        return "SUCCESS";
      }
    });
    Assert.assertEquals(3, atomicInteger.get());
    Assert.assertEquals("SUCCESS", result);
  }

  @Test(expected = IOException.class)
  public void testFailedWithoutHandleIOException() throws Throwable {
    RetryTemplate template = RetryTemplate.builder().maxAttempts(3)
        .retryOn(RuntimeException.class)
        .build();

    AtomicInteger atomicInteger = new AtomicInteger(0);
    template.execute(new RetryCallback<String, Throwable>() {
      @Override
      public String doWithRetry(RetryContext context) throws Throwable {
        if (atomicInteger.getAndIncrement() <= 1) {
          throw new IOException("error");
        }
        return "SUCCESS";
      }
    });
  }

  @Test
  public void testSuccessWhenHandleIOException() throws Throwable {
    RetryTemplate template = RetryTemplate.builder().maxAttempts(2)
        .retryOn(IOException.class)
        .build();

    AtomicInteger atomicInteger = new AtomicInteger(0);
    template.execute(new RetryCallback<String, Throwable>() {
      @Override
      public String doWithRetry(RetryContext context) throws Throwable {
        if (atomicInteger.getAndIncrement() < 1) {
          throw new IOException("error");
        }
        return "SUCCESS";
      }
    });
  }

  @Test(expected = IOException.class)
  public void testFailedWhenHandleIOExceptionButReachMaxAttempt() throws Throwable {
    RetryTemplate template = RetryTemplate.builder().maxAttempts(1)
        .retryOn(IOException.class)
        .build();

    AtomicInteger atomicInteger = new AtomicInteger(0);
    template.execute(new RetryCallback<String, Throwable>() {
      @Override
      public String doWithRetry(RetryContext context) throws Throwable {
        if (atomicInteger.getAndIncrement() <= 1) {
          throw new IOException("error");
        }
        return "SUCCESS";
      }
    });
  }

  @Test(expected = IOException.class)
  public void testRetryWithBackOffPeriod() throws Throwable {
    RetryTemplate template = RetryTemplate.builder().maxAttempts(2)
        .retryOn(IOException.class)
        .backOffPeriod(TimeUnit.SECONDS.toMillis(1))
        .build();

    AtomicInteger atomicInteger = new AtomicInteger(0);
    template.execute(new RetryCallback<String, Throwable>() {
      @Override
      public String doWithRetry(RetryContext context) throws Throwable {
        if (atomicInteger.getAndIncrement() <= 1) {
          throw new IOException("error");
        }
        return "SUCCESS";
      }
    });

  }

  @Test
  public void testTimeUnitConvert(){
    Assert.assertEquals(1000, TimeUnit.SECONDS.toMillis(1));
    Assert.assertEquals(1000, TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS));
  }

}
