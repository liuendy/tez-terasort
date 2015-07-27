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

package terasort.gen;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import terasort.Constants;
import terasort.io.TeraOutputFormat;
import terasort.utils.Utils;

import java.io.IOException;
import java.util.Set;

//TODO: Incomplete. Use mapreduce TeraGen as of now until this is fixed.
public class TezTeraGen extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(TezTeraGen.class);

  @Override public int run(String[] args) throws Exception {
    LOG.info("starting");

    String outputLocation = args[1];

    TezConfiguration tezConf = new TezConfiguration(getConf());
    DAG teragenDAG = DAG.create("TeraGenData-Tez");
    teragenDAG.addTaskLocalFiles(Utils.getLocalResources(tezConf));

    Vertex scanVertex = Vertex.create("DataGenVertex", ProcessorDescriptor.create(TeraGenProcessor
        .class.getName()), -1);
    scanVertex.addDataSource("input",
        MRInput.createConfigBuilder(tezConf, RangeInputFormat.class).build());
    scanVertex.addDataSink("sink", MROutput.createConfigBuilder(tezConf,
        TeraOutputFormat.class, outputLocation).build());
    teragenDAG.addVertex(scanVertex);

    TezClient client = TezClient.create("TeraGenData-Tez", tezConf);
    client.start();
    client.waitTillReady();

    DAGClient dagClient = client.submitDAG(teragenDAG);
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

  private static void usage() throws IOException {
    System.err.println("teragen <num rows> <output dir>");
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      usage();
      return;
    }

    Configuration conf = new TezConfiguration();

    //For local mode testing
    conf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    conf.set("fs.default.name", "file:///");
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

    conf.set(Constants.NUM_ROWS, args[0]);

    ToolRunner.run(conf, new TezTeraGen(), args);
  }

}
