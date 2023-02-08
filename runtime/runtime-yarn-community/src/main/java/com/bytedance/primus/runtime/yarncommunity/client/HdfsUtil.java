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

package com.bytedance.primus.runtime.yarncommunity.client;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsUtil.class);

  public static URI resolveURI(String file) throws URISyntaxException {
    URI uri = new URI(file);
    if (uri.getScheme() != null) {
      return uri;
    }
    if (uri.getFragment() != null) {
      URI absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI();
      return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment());
    }
    return new File(file).getAbsoluteFile().toURI();
  }

  public static Path getQualifiedLocalPath(URI localURI, Configuration hadoopConf)
      throws IOException, URISyntaxException {
    URI qualifiedURI = localURI;
    if (localURI.getScheme() == null) {
      qualifiedURI =
          new URI(FileSystem.getLocal(hadoopConf).makeQualified(new Path(localURI)).toString());
    }
    return new Path(qualifiedURI);
  }

  public static void addResource(
      FileSystem fs,
      Path path,
      String linkname,
      Map<String, LocalResource> cacheFiles
  ) throws IOException {
    LocalResource rsrc = new LocalResourcePBImpl();
    FileStatus rsrcStat = fs.getFileStatus(path);
    URL resource = ConverterUtils.getYarnUrlFromPath(fs.resolvePath(rsrcStat.getPath()));
    LOG.info("Add resource[" + resource + "], path[" + path + "], linkname[" + linkname + "]");
    rsrc.setResource(resource);
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(LocalResourceType.FILE);
    rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    cacheFiles.put(linkname, rsrc);
  }

  public static String buildLinkname(Path path, URI localURI, String destName, String targetDir) {
    String linkname;
    linkname = "";
    if (targetDir != null) {
      linkname += targetDir + "/";
    }
    if (destName != null) {
      linkname += destName;
    } else if (localURI.getFragment() != null) {
      linkname += localURI.getFragment();
    } else {
      linkname += path.getName();
    }

    if (linkname.isEmpty()) {
      linkname = path.getParent().getName();
    }

    return linkname;
  }

  public static Boolean addDistributedUri(URI uri,
      Set<String> distributedUris,
      Set<String> distributedNames) {
    String uriStr = uri.toString();
    String fileName = new File(uri.getPath()).getName();
    if (distributedUris.contains(uriStr)) {
      LOG.warn("Same path resource " + uri + " added multiple times to distributed cache.");
      return false;
    } else if (distributedNames.contains(fileName)) {
      LOG.warn("Same name resource " + uri + " added multiple times to distributed cache");
      return false;
    } else {
      distributedUris.add(uriStr);
      distributedNames.add(fileName);
      return true;
    }
  }
}