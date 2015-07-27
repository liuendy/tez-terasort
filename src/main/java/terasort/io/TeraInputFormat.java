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
package terasort.io;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * An input format that reads the first 10 characters of each line as the key
 * and the rest of the line as the value. Both key and value are represented
 * as Text.
 */
public class TeraInputFormat extends FileInputFormat<Text, Text> {

  public static final int KEY_LENGTH = 10;
  public static final int VALUE_LENGTH = 90;
  private static MRJobConfig lastContext = null;
  private static List<InputSplit> lastResult = null;



  static class TeraRecordReader extends RecordReader<Text, Text> {
    private FSDataInputStream in;
    private long offset;
    private long length;
    private static final int RECORD_LENGTH = KEY_LENGTH + VALUE_LENGTH;
    private byte[] buffer = new byte[RECORD_LENGTH];
    private Text key;
    private Text value;

    public TeraRecordReader() throws IOException {
    }

    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      Path p = ((FileSplit) split).getPath();
      FileSystem fs = p.getFileSystem(context.getConfiguration());
      in = fs.open(p);
      long start = ((FileSplit) split).getStart();
      // find the offset to start at a record boundary
      offset = (RECORD_LENGTH - (start % RECORD_LENGTH)) % RECORD_LENGTH;
      in.seek(start + offset);
      length = ((FileSplit) split).getLength();
    }

    public void close() throws IOException {
      in.close();
    }

    public Text getCurrentKey() {
      return key;
    }

    public Text getCurrentValue() {
      return value;
    }

    public float getProgress() throws IOException {
      return (float) offset / length;
    }

    public boolean nextKeyValue() throws IOException {
      if (offset >= length) {
        return false;
      }
      int read = 0;
      while (read < RECORD_LENGTH) {
        long newRead = in.read(buffer, read, RECORD_LENGTH - read);
        if (newRead == -1) {
          if (read == 0) {
            return false;
          } else {
            throw new EOFException("read past eof");
          }
        }
        read += newRead;
      }
      if (key == null) {
        key = new Text();
      }
      if (value == null) {
        value = new Text();
      }
      key.set(buffer, 0, KEY_LENGTH);
      value.set(buffer, KEY_LENGTH, VALUE_LENGTH);
      offset += RECORD_LENGTH;
      return true;
    }
  }

  @Override
  public RecordReader<Text, Text>
  createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    return new TeraRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    if (job == lastContext) {
      return lastResult;
    }
    long t1, t2, t3;
    t1 = System.currentTimeMillis();
    lastContext = job;
    lastResult = super.getSplits(job);
    t2 = System.currentTimeMillis();
    System.out.println("Spent " + (t2 - t1) + "ms computing base-splits.");
    return lastResult;
  }
}