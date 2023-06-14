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

package com.bytedance.primus.apiserver.client.models;

import static com.bytedance.primus.apiserver.utils.Constants.KIND_KEY;
import static com.bytedance.primus.apiserver.utils.Constants.META_KEY;
import static com.bytedance.primus.apiserver.utils.Constants.SPEC_KEY;
import static com.bytedance.primus.apiserver.utils.Constants.STATUS_KEY;

import com.bytedance.primus.apiserver.records.Meta;
import com.bytedance.primus.apiserver.records.Resource;
import com.bytedance.primus.apiserver.records.impl.ResourceImpl;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.Any;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractApiType<T extends ApiType, TSpec, TStatus>
    implements ApiType<T, TSpec, TStatus> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractApiType.class);

  protected Resource toResourceImpl(String kind, Meta meta, Any spec, Any status) {
    Resource resource = new ResourceImpl();
    resource.setKind(kind);
    if (meta != null) {
      resource.setMeta(meta);
    }
    if (spec != null) {
      resource.setSpec(spec);
    }
    if (status != null) {
      resource.setStatus(status);
    }
    return resource;
  }

  protected String toStringImpl(String kind, MessageOrBuilder meta, MessageOrBuilder spec,
      MessageOrBuilder status) {
    JsonFormat.Printer printer =
        JsonFormat.printer().omittingInsignificantWhitespace().includingDefaultValueFields();
    JsonParser jsonParser = new JsonParser();
    JsonObject jsonObject = new JsonObject();
    try {
      jsonObject.addProperty(KIND_KEY, kind);
      if (meta != null) {
        jsonObject.add(META_KEY, getJsonElement(printer, jsonParser, meta));
      } else {
        jsonObject.add(META_KEY, new JsonObject());
      }
      if (spec != null) {
        jsonObject.add(SPEC_KEY, getJsonElement(printer, jsonParser, spec));
      } else {
        jsonObject.add(SPEC_KEY, new JsonObject());
      }
      if (status != null) {
        jsonObject.add(STATUS_KEY, getJsonElement(printer, jsonParser, status));
      } else {
        jsonObject.add(STATUS_KEY, new JsonObject());
      }
    } catch (Exception e) {
      LOG.warn("Failed to get string for resource kind " + kind, e);
      return "Failed to get string for resource kind " + kind + ", exception " + e;
    }
    return jsonObject.toString();
  }

  private JsonElement getJsonElement(JsonFormat.Printer printer, JsonParser jsonParser,
      MessageOrBuilder messageOrBuilder) {
    try {
      return jsonParser.parse(printer.print(messageOrBuilder));
    } catch (Exception e) {
      LOG.debug("Failed to apply json parser to message "
          + TextFormat.printToString(messageOrBuilder), e);
      return new JsonPrimitive(TextFormat.printToString(messageOrBuilder));
    }
  }
}
