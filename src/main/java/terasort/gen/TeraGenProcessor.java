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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import terasort.gen.utils.GenSort;
import terasort.gen.utils.Random16;
import terasort.gen.utils.Unsigned16;
import terasort.io.TeraInputFormat;

import java.security.SecureRandom;
import java.util.zip.Checksum;

public class TeraGenProcessor extends SimpleMRProcessor {

  private Text key = new Text();
  private Text value = new Text();
  private Unsigned16 rand = null;
  private Unsigned16 rowId = null;
  private Unsigned16 checksum = new Unsigned16();
  private Checksum crc32 = new PureJavaCrc32();
  private Unsigned16 total = new Unsigned16();
  private static final Unsigned16 ONE = new Unsigned16(1);
  private byte[] buffer = new byte[TeraInputFormat.KEY_LENGTH + TeraInputFormat.VALUE_LENGTH];
  private SecureRandom rnd = new SecureRandom();

  public TeraGenProcessor(ProcessorContext context) {
    super(context);
  }

  @Override public void run() throws Exception {

    if (rand == null) {
      rowId = new Unsigned16(rnd.nextLong());
      rand = Random16.skipAhead(rowId);
    }
    //TODO: For testing. do it properly
    for(int i=0;i<1000000;i++) {
      Random16.nextRand(rand);
      GenSort.generateRecord(buffer, rand, rowId);
      key.set(buffer, 0, TeraInputFormat.KEY_LENGTH);
      value.set(buffer, TeraInputFormat.KEY_LENGTH,
          TeraInputFormat.VALUE_LENGTH);

      KeyValueWriter writer = (KeyValueWriter) getOutputs().get("sink").getWriter();

      writer.write(key, value);

      crc32.reset();
      crc32.update(buffer, 0, TeraInputFormat.KEY_LENGTH + TeraInputFormat.VALUE_LENGTH);
      checksum.set(crc32.getValue());
      total.add(checksum);
      rowId.add(ONE);
    }
  }
}
