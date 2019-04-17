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

import com.bytedance.primus.io.file.recordreader.ValueWrapper;
import java.io.IOException;
import java.nio.BufferOverflowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.BytesWritable;

public class PlainMessageBuilder extends MessageBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(PlainMessageBuilder.class);

  public PlainMessageBuilder(int bufferSize) {
    super(bufferSize);
  }

  @Override
  protected void writeKey(Object key) throws IOException, BufferOverflowException {

  }

  @Override
  protected void writeValue(Object value) throws IOException, BufferOverflowException {
    ValueWrapper valueWrapper = (ValueWrapper) value;
    byte[] instanceTypeBytes = intToByteArray(valueWrapper.getInstanceType().value());
    byte[] sourceBytes = intToByteArray(valueWrapper.getSource().hashCode());

    // write out by little endian
    buffer.put(instanceTypeBytes[3]);
    buffer.put(sourceBytes[3]);
    buffer.put(sourceBytes[2]);
    buffer.put(sourceBytes[1]);

    buffer.putInt(0);  // reserve bits
    BytesWritable bytesWritable = ((BytesWritable) (valueWrapper.getRealValue()));
    buffer.putLong(Long.reverseBytes(bytesWritable.getLength()));
    buffer.put(bytesWritable.getBytes(), 0, bytesWritable.getLength());
  }

  public static final byte[] intToByteArray(int value) {
    return new byte[]{
        (byte) (value >>> 24),
        (byte) (value >>> 16),
        (byte) (value >>> 8),
        (byte) value};
  }
}
