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
import org.apache.tez.common.TezUtils;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import terasort.Constants;
import terasort.TeraSort;

import java.io.IOException;

/**
 * Scan incoming data.
 * - Send some of it to sampler vertex via broadcast
 * - Send another copy to partition vertex
 */
public class ScanProcessor extends SimpleMRProcessor {


  final long recordsPerSample;

  public ScanProcessor(ProcessorContext context) throws IOException {
    super(context);

    Configuration conf = TezUtils.createConfFromUserPayload(context.getUserPayload());
    long sampleSize = conf.getLong(Constants.SAMPLE_SIZE, 100000);
    int samples = Math.min(conf.getInt(Constants.NUM_PARTITIONS, 2), context.getVertexParallelism());
    recordsPerSample = sampleSize / samples;
  }

  @Override public void run() throws Exception {
    long records = 0;
    KeyValueReader reader = (KeyValueReader) getInputs().get(TeraSort.SOURCE).getReader();
    KeyValuesWriter writer =
        (KeyValuesWriter) getOutputs().get(TeraSort.PARTITION_VERTEX).getWriter();
    KeyValuesWriter sampleWriter =
        (KeyValuesWriter) getOutputs().get(TeraSort.SAMPLER_VERTEX).getWriter();
    while (reader.next()) {
      Object key = reader.getCurrentKey();
      Object value = reader.getCurrentValue();
      writer.write(key, value);
      records++;
      if (recordsPerSample >= records) {
        sampleWriter.write(key, NullWritable.get());
      }
    }
  }
}
