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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class TextMessageBuilder extends MessageBuilder {

  private static final byte[] FIELD_SEPERATOR = "\t".getBytes();
  private static final byte[] LINE_SEPERATOR = "\n".getBytes();

  public TextMessageBuilder(int bufferSize) {
    super(bufferSize);
  }

  @Override
  protected void writeKey(Object key) throws IOException {
    writeUTF8(key);
    buffer.put(FIELD_SEPERATOR, 0, FIELD_SEPERATOR.length);
  }

  @Override
  protected void writeValue(Object value) throws IOException {
    writeUTF8(value);
    buffer.put(LINE_SEPERATOR, 0, LINE_SEPERATOR.length);
  }

  private void writeUTF8(Object object) throws IOException {
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
      bval = sval.getBytes("UTF-8");
      valSize = bval.length;
    }
    buffer.put(bval, 0, valSize);
  }
}
