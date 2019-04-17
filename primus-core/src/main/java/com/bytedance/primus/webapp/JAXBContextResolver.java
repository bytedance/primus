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

import com.bytedance.primus.webapp.dao.AppEntry;
import com.bytedance.primus.webapp.dao.AppList;
import com.bytedance.primus.webapp.dao.AppSnapshot;
import com.google.inject.Singleton;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

@Singleton
@Provider
public class JAXBContextResolver implements ContextResolver<JAXBContext> {

  private JAXBContext context;
  private final Set<Class> types;

  // you have to specify all the dao classes here
  private final Class[] cTypes = {AppList.class, AppEntry.class, AppSnapshot.class};

  public JAXBContextResolver() throws Exception {
    this.types = new HashSet<Class>(Arrays.asList(cTypes));
    this.context = new JSONJAXBContext(JSONConfiguration.natural()
        .rootUnwrapping(false).build(), cTypes);
  }

  @Override
  public JAXBContext getContext(Class<?> objectType) {
    return (types.contains(objectType)) ? context : null;
  }
}
