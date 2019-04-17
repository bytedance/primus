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

package com.bytedance.primus.io.messagebuilder;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public abstract class MessageBuilder {

  public static final int DEFAULT_MESSAGE_BUFFER_SIZE = 4 * 1024 * 1024;

  private int flushSize;
  protected ByteBuffer buffer;
  private int messageNum;

  public MessageBuilder(int bufferSize) {
    int maxMessageSize = bufferSize / 16;
    flushSize = bufferSize - maxMessageSize;
    buffer = ByteBuffer.allocate(bufferSize);
    messageNum = 0;
  }

  public byte[] build() throws IOException {
    return buffer.array();
  }

  public int size() {
    return buffer.position();
  }

  public void reset() {
    messageNum = 0;
    buffer.clear();
  }

  public void add(Object key, Object value) throws IOException, BufferOverflowException {
    messageNum += 1;
    writeKey(key);
    writeValue(value);
  }

  public boolean needFlush() {
    return buffer.position() > flushSize;
  }

  public int getMessageNum() {
    return messageNum;
  }

  protected abstract void writeKey(Object key) throws IOException, BufferOverflowException;

  protected abstract void writeValue(Object value) throws IOException, BufferOverflowException;
}
