/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package terasort.utils;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.TezConfiguration;
import terasort.TeraSort;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.UUID;

public class Utils {

  public static Map<String, LocalResource> getLocalResources(TezConfiguration tezConf) throws
      IOException, URISyntaxException {
    Map<String, LocalResource> localResources = Maps.newHashMap();
    Path stagingDir = TezCommonUtils.getTezBaseStagingPath(tezConf);

    // staging dir
    FileSystem fs = FileSystem.get(tezConf);
    String uuid = UUID.randomUUID().toString();
    Path jobJar = new Path(stagingDir, uuid + "_job.jar");
    if (fs.exists(jobJar)) {
      fs.delete(jobJar, false);
    }
    fs.copyFromLocalFile(getCurrentJarURL(), jobJar);

    localResources.put(uuid + "_job.jar", createLocalResource(fs, jobJar));
    return localResources;
  }

  public static Path getCurrentJarURL() throws URISyntaxException {
    return new Path(TeraSort.class.getProtectionDomain().getCodeSource()
        .getLocation().toURI());
  }

  public static LocalResource createLocalResource(FileSystem fs, Path file) throws IOException {
    final LocalResourceType type = LocalResourceType.FILE;
    final LocalResourceVisibility visibility = LocalResourceVisibility.APPLICATION;
    FileStatus fstat = fs.getFileStatus(file);
    org.apache.hadoop.yarn.api.records.URL resourceURL = ConverterUtils.getYarnUrlFromPath(file);
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();
    LocalResource lr = Records.newRecord(LocalResource.class);
    lr.setResource(resourceURL);
    lr.setType(type);
    lr.setSize(resourceSize);
    lr.setVisibility(visibility);
    lr.setTimestamp(resourceModificationTime);
    return lr;
  }

}
