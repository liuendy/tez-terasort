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

import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import terasort.TeraSort;

/**
 * Scan and dump results in sink
 */
public class ReduceProcessor extends SimpleMRProcessor {
  public ReduceProcessor(ProcessorContext context) {
    super(context);
  }

  @Override public void run() throws Exception {
    KeyValuesReader reader =
        (KeyValuesReader) getInputs().get(TeraSort.PARTITION_VERTEX).getReader();
    KeyValueWriter writer = (KeyValueWriter) getOutputs().get(TeraSort.SINK).getWriter();

    while (reader.next()) {
      Object key = reader.getCurrentKey();
      for (Object val : reader.getCurrentValues()) {
        writer.write(key, val);
      }
    }
  }
}
