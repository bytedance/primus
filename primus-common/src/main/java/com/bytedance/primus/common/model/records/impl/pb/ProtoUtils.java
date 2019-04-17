/*
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
 *
 * This file may have been modified by Bytedance Inc.
 */

package com.bytedance.primus.common.model.records.impl.pb;

import com.bytedance.primus.common.model.protocolrecords.ResourceTypes;
import com.bytedance.primus.common.model.records.Resource;
import com.bytedance.primus.common.model.records.ValueRanges;
import com.bytedance.primus.common.proto.ModelProtos.ResourceProto;
import com.bytedance.primus.common.proto.ModelProtos.ResourceTypesProto;
import com.bytedance.primus.common.proto.ModelProtos.ValueRangesProto;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;


public class ProtoUtils {


  /*
   * ByteBuffer
   */
  public static ByteBuffer convertFromProtoFormat(ByteString byteString) {
    int capacity = byteString.asReadOnlyByteBuffer().rewind().remaining();
    byte[] b = new byte[capacity];
    byteString.asReadOnlyByteBuffer().get(b, 0, capacity);
    return ByteBuffer.wrap(b);
  }

  public static ByteString convertToProtoFormat(ByteBuffer byteBuffer) {
//    return ByteString.copyFrom((ByteBuffer)byteBuffer.duplicate().rewind());
    int oldPos = byteBuffer.position();
    byteBuffer.rewind();
    ByteString bs = ByteString.copyFrom(byteBuffer);
    byteBuffer.position(oldPos);
    return bs;
  }

  /*
   * ResourceTypes
   */
  public static ResourceTypesProto convertToProtoFormat(ResourceTypes e) {
    return ResourceTypesProto.valueOf(e.name());
  }


  public static ValueRangesProto convertToProtoFormat(ValueRanges e) {
    return ((ValueRangesPBImpl) e).getProto();
  }

  /*
   * Resource
   */
  public static synchronized ResourceProto convertToProtoFormat(Resource r) {
    return ResourcePBImpl.getProto(r);
  }

}
