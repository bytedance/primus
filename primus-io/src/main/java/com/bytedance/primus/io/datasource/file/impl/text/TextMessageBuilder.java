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

package com.bytedance.primus.io.datasource.file.impl.text;

import com.bytedance.primus.io.messagebuilder.MessageBuilder;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class TextMessageBuilder extends MessageBuilder {

  private static final byte[] LINE_SEPARATOR = "\n".getBytes();

  public TextMessageBuilder(int bufferSize) {
    super(bufferSize);
  }

  @Override
  protected void writeKey(Object key) {
  }

  @Override
  protected void writeValue(Object value) {
    writeUTF8(value);
    buffer.put(LINE_SEPARATOR, 0, LINE_SEPARATOR.length);
  }

  private void writeUTF8(Object object) {
    byte[] bval;
    int valSize;
    if (object instanceof BytesWritable) {
      BytesWritable val = (BytesWritable) object;
      bval = val.getBytes();
      valSize = val.getLength();
    } else if (object instanceof Text) {
      Text val = (Text) object;
      bval = val.getBytes();
      valSize = val.getLength();
    } else {
      String sval = object.toString();
      bval = sval.getBytes(StandardCharsets.UTF_8);
      valSize = bval.length;
    }
    buffer.put(bval, 0, valSize);
  }
}
