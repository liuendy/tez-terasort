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
package terasort.processors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.tez.common.TezUtils;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.common.writers.UnorderedPartitionedKVWriter;
import terasort.TeraSort;
import terasort.utils.TextSampler;

import java.io.IOException;

/**
 * This just gets the sample data and based on this, sends data to TextSampler for part stats
 * generation.
 */
public class SamplerProcessor extends SimpleMRProcessor {

  int partitions = 0;
  TextSampler sampler;

  public SamplerProcessor(ProcessorContext context) throws IOException {
    super(context);
    Configuration conf = TezUtils.createConfFromUserPayload(context.getUserPayload());
    partitions = conf.getInt(TeraSort.REDUCERS, 100);
    sampler = new TextSampler();
  }

  @Override public void run() throws Exception {
    KeyValueReader reader = (KeyValueReader) (getInputs().get(TeraSort.SCAN_VERTEX).getReader());
    UnorderedPartitionedKVWriter writer =
        (UnorderedPartitionedKVWriter) getOutputs().get(TeraSort.PARTITION_VERTEX).getWriter();
    while (reader.next()) {
      sampler.addKey((Text) reader.getCurrentKey());
    }

    /**
     * Write all partition details in partitionVertex (broadcast)
     */
    Text[] splits = sampler.createPartitions(partitions);
    for (Text split : splits) {
      writer.write(split, NullWritable.get());
    }
  }
}
