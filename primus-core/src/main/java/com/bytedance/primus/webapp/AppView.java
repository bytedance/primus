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
package com.bytedance.primus.webapp;

import java.io.InputStream;
import java.nio.charset.Charset;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.view.TextView;

/**
 * A view that should be used as the base class for all history server pages.
 */
public class AppView extends TextView {

  protected AppView() {
    this(null);
  }

  protected AppView(ViewContext ctx) {
    super(ctx, MimeType.HTML);
  }

  @Override
  public void render() {
    // Populate content
    String content;
    try (InputStream inputStream = this.getClass()
        .getClassLoader()
        .getResourceAsStream("webapps/primus/index.html")
    ) {
      content = inputStream != null
          ? IOUtils.toString(inputStream, Charset.defaultCharset())
          : "Missing content!";

    } catch (Exception e) {
      content = "Failed to load static resources";
      LOG.error(content, e);
    }

    // Return the content
    writer().print(content);
  }
}
