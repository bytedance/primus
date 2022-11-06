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

package com.bytedance.primus.io.datasource.file.impl.customized;

import com.bytedance.primus.io.messagebuilder.MessageBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.io.Text;

public class CustomizedMessageBuilder extends MessageBuilder {

  private static final byte[] SUFFIX_SEPARATOR = "-".getBytes();
  private static final byte[] FIELD_SEPARATOR = "\t".getBytes();
  private static final byte[] LINE_SEPARATOR = "\n".getBytes();

  private final byte[] keySuffixBytes;
  private final byte[] valSuffixBytes;

  public CustomizedMessageBuilder(int bufferSize, String keySuffix, String valSuffix) {
    super(bufferSize);
    this.keySuffixBytes = keySuffix.getBytes(StandardCharsets.UTF_8);
    this.valSuffixBytes = valSuffix.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  protected void writeKey(Object key) throws IOException {
    buffer.put(((Text) key).getBytes());
    buffer.put(SUFFIX_SEPARATOR);
    buffer.put(keySuffixBytes);
    buffer.put(FIELD_SEPARATOR);
  }

  @Override
  protected void writeValue(Object val) throws IOException {
    buffer.put(((Text) val).getBytes());
    buffer.put(SUFFIX_SEPARATOR);
    buffer.put(valSuffixBytes);
    buffer.put(LINE_SEPARATOR);
  }
}
