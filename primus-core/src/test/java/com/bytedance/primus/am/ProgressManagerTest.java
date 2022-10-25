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

package com.bytedance.primus.am;

import static org.junit.Assert.assertEquals;

import com.bytedance.primus.am.datastream.DataStreamManager;
import com.bytedance.primus.am.datastream.DataStreamManagerEvent;
import com.bytedance.primus.am.datastream.DataStreamManagerEventType;
import com.bytedance.primus.am.progress.FileProgressManager;
import com.bytedance.primus.am.progress.ProgressManager;
import com.bytedance.primus.apiserver.proto.DataProto;
import com.bytedance.primus.apiserver.records.DataSpec;
import com.bytedance.primus.apiserver.records.impl.DataSpecImpl;
import com.bytedance.primus.common.event.AsyncDispatcher;
import com.bytedance.primus.common.service.Service.STATE;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.google.protobuf.util.JsonFormat;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

public class ProgressManagerTest {

  private class TestAMContext extends AMContext {

    public TestAMContext(PrimusConf primusConf) throws IOException {
      super(primusConf);
    }
  }

  private DataSpec createDummyDataSpec() throws Exception {
    String dataSpecJsonPath = "src/test/resources/data_spec.json";
    InputStream in = new FileInputStream(dataSpecJsonPath);
    Reader reader = new InputStreamReader(in);
    JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
    DataProto.DataSpec.Builder builder = DataProto.DataSpec.newBuilder();
    parser.merge(reader, builder);
    DataSpec dataSpec = new DataSpecImpl(builder.build());
    return dataSpec;
  }

  @Test
  public void testDisplayProgress() throws Exception {
    int totalDatastreamCount = 2;
    Configuration emptyConf = new BaseConfiguration();
    PrimusConf emptyPrimusConf = PrimusConf.newBuilder().build();
    AMContext context = new TestAMContext(emptyPrimusConf);
    DataSpec dataSpec = createDummyDataSpec();
    dataSpec = dataSpec.setTotalDatastreamCount(totalDatastreamCount);
    DataStreamManager dsManager = new DataStreamManager(context);
    dsManager.setDataSpec(dataSpec);
    dsManager.init(emptyConf);
    dsManager.start();
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.register(DataStreamManagerEventType.class, dsManager);
    context.setDispatcher(dispatcher);
    context.getDispatcher().getEventHandler().handle(new DataStreamManagerEvent(
        DataStreamManagerEventType.DATA_STREAM_CREATED,
        dataSpec,
        1));
    context.setDataStreamManager(dsManager);
    ProgressManager progressManager = new FileProgressManager(FileProgressManager.class.getName(),
        context);
    progressManager.init(emptyConf);
    progressManager.start();
    assertEquals(STATE.STARTED, progressManager.getServiceState());
    assertEquals(1, dsManager.getTaskManagerMap().size());
    assertEquals(totalDatastreamCount, dsManager.getDataSpec().getTotalDatastreamCount());
  }
}
