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
package terasort;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import terasort.io.TeraInputFormat;
import terasort.io.TeraOutputFormat;
import terasort.processors.PartitionProcessor;
import terasort.processors.ReduceProcessor;
import terasort.processors.SamplerProcessor;
import terasort.processors.ScanProcessor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Generates the sampled split points, launches the job, and waits for it to
 * finish.
 * <p/>
 * To run the program:
 * <b>yarn jar tez-terasort-1.0-SNAPSHOT.jar terasort.TeraSort in-dir out-dir partitions</b>
 */

/**
 * TODO: MAINLY TESTED IN LOCAL MODE.  NEED TO TRY IN REAL CLUSTER FOR JARS ETC.
 * TODO: REWIRE VERTICES TO MATCH http://people.apache.org/~gopalv/tez_sort.png
 * TODO: IMPLEMENT CUSTOM COMPARATOR/SERIALIZATION
 * TODO: ADD OPTIONS FOR FINAL REPLICATION
 */
public class TeraSort extends Configured implements Tool {

  public static final String SOURCE = "source";
  public static final String SINK = "sink";
  public static final String SCAN_VERTEX = "scanVertex";
  public static final String SAMPLER_VERTEX = "samplerVertex";
  public static final String PARTITION_VERTEX = "partitionVertex";
  public static final String SINK_VERTEX = "sinkVertex";

  private static final Log LOG = LogFactory.getLog(TeraSort.class);

  public static final String REDUCERS = "final.reducers";

  Map<String, LocalResource> getLocalResources(TezConfiguration tezConf) throws
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

  Path getCurrentJarURL() throws URISyntaxException {
    return new Path(TeraSort.class.getProtectionDomain().getCodeSource()
        .getLocation().toURI());
  }

  LocalResource createLocalResource(FileSystem fs, Path file) throws IOException {
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

  public int run(String[] args) throws Exception {
    LOG.info("starting");

    String inputPath = args[0];
    String outputPath = args[1];
    int finalReducers = Integer.parseInt(args[2]);
    getConf().setInt(REDUCERS, finalReducers);

    /**
     * Currently doing the following.
     *
     *                   / ------------------> PARTITION_VERTEX ---> SINK_VERTEX --> SINK
     *  SCAN_VERTEX ---->                      /
     *                   \ --> SAMPLER_VERTEX /
     *
     *
     *
     * SCAN_VERTEX --> PARTITION_VERTEX ( One to One )
     * SCAN_VERTEX --> SAMPLER_VERTEX ( broadcast )
     * SAMPLER_VERTEX --> PARTITION_VERTEX ( broadcast )
     * PARTTION_VERTEX --> SINK_VERTEX ( scatter-gather)
     *
     * Eventually need to get to http://people.apache.org/~gopalv/tez_sort.png . This should be
     * possible as it would be re-arranging the vertices.
     */

    TezConfiguration tezConf = new TezConfiguration(getConf());
    DAG sortDAG = DAG.create("TeraSort-Tez");

    // sortDAG.addTaskLocalFiles(Utils.getLocalResources(classList, tezConf));
    sortDAG.addTaskLocalFiles(getLocalResources(tezConf));

    Vertex scanVertex = Vertex.create(SCAN_VERTEX, ProcessorDescriptor.create(ScanProcessor
        .class.getName()).setUserPayload(TezUtils.createUserPayloadFromConf(tezConf)), -1);
    scanVertex.addDataSource(SOURCE,
        MRInput.createConfigBuilder(tezConf, TeraInputFormat.class, inputPath).build());
    sortDAG.addVertex(scanVertex);

    Vertex samplerVertex =
        Vertex.create(SAMPLER_VERTEX, ProcessorDescriptor.create(SamplerProcessor
            .class.getName()).setUserPayload(TezUtils.createUserPayloadFromConf(tezConf)), 1);
    sortDAG.addVertex(samplerVertex);

    Vertex partitionVertex = Vertex.create(PARTITION_VERTEX, ProcessorDescriptor.create
        (PartitionProcessor.class.getName()), -1);
    sortDAG.addVertex(partitionVertex);

    Vertex sinkVertex = Vertex.create(SINK_VERTEX, ProcessorDescriptor.create(ReduceProcessor
        .class.getName()), finalReducers);
    sinkVertex.addDataSink(SINK, MROutput.createConfigBuilder(tezConf,
        TeraOutputFormat.class, outputPath).build());
    sortDAG.addVertex(sinkVertex);

    //Broadcast
    Edge scanToSampler = Edge.create(scanVertex, samplerVertex, UnorderedKVEdgeConfig
        .newBuilder(Text.class.getName(), NullWritable.class.getName()).build()
        .createDefaultBroadcastEdgeProperty());
    sortDAG.addEdge(scanToSampler);

    //Broadcast
    Edge samplerToPartition = Edge.create(samplerVertex, partitionVertex, UnorderedKVEdgeConfig
        .newBuilder(Text.class.getName(), NullWritable.class.getName()).build()
        .createDefaultBroadcastEdgeProperty());
    sortDAG.addEdge(samplerToPartition);

    //1:1
    Edge scannerToPartition = Edge.create(scanVertex, partitionVertex, UnorderedKVEdgeConfig
        .newBuilder(Text.class.getName(), Text.class.getName()).build()
        .createDefaultOneToOneEdgeProperty());
    sortDAG.addEdge(scannerToPartition);

    //ScatterGather
    Edge partitionToSink = Edge.create(partitionVertex, sinkVertex, OrderedPartitionedKVEdgeConfig
        .newBuilder(Text.class.getName(), Text.class.getName(),
            PartitionProcessor.TotalOrderPartitioner.class.getName())
        .build().createDefaultEdgeProperty());
    sortDAG.addEdge(partitionToSink);

    //TODO: Consider setting number of replication for output
    TezClient client = TezClient.create("TestDAGSort", tezConf);
    client.start();
    client.waitTillReady();
    client.addAppMasterLocalFiles(getLocalResources(tezConf));

    DAGClient dagClient = client.submitDAG(sortDAG);
    Set<StatusGetOpts> getOpts = Sets.newHashSet();
    getOpts.add(StatusGetOpts.GET_COUNTERS);

    DAGStatus dagStatus;
    dagStatus = dagClient.waitForCompletionWithStatusUpdates(getOpts);

    System.out.println(dagStatus.getDAGCounters());
    if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
      System.out.println("DAG diagnostics: " + dagStatus.getDiagnostics());
      return -1;
    }
    return 0;
  }

  /**
   * @param args
   * e.g yarn jar tez-terasort-1.1.0-SNAPSHOT.jar terasort.TeraSort /user/rajesh/teragen-200/ /user/rajesh/tera-output-200/ 100 100
   */
  public static void main(String[] args) throws Exception {

    Configuration conf = new TezConfiguration();

    /*
    //For local mode testing
    conf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    conf.set("fs.default.name", "file:///");
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
     */

    int res = ToolRunner.run(conf, new TeraSort(), args);
    System.exit(res);
  }

}

