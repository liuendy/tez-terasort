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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import terasort.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An input format that assigns ranges of longs to each mapper.
 */
public class RangeInputFormat
    extends InputFormat<LongWritable, NullWritable> {

  private static final Log LOG = LogFactory.getLog(RangeInputFormat.class);

  /**
   * An input split consisting of a range on numbers.
   */
  public static class RangeInputSplit extends InputSplit implements Writable {
    long firstRow;
    long rowCount;

    public RangeInputSplit() {
    }

    public RangeInputSplit(long offset, long length) {
      firstRow = offset;
      rowCount = length;
    }

    public long getLength() throws IOException {
      return 0;
    }

    public String[] getLocations() throws IOException {
      return new String[] { };
    }

    public void readFields(DataInput in) throws IOException {
      firstRow = WritableUtils.readVLong(in);
      rowCount = WritableUtils.readVLong(in);
    }

    public void write(DataOutput out) throws IOException {
      WritableUtils.writeVLong(out, firstRow);
      WritableUtils.writeVLong(out, rowCount);
    }
  }

  /**
   * A record reader that will generate a range of numbers.
   */
  public static class RangeRecordReader
      extends RecordReader<LongWritable, NullWritable> {
    long startRow;
    long finishedRows;
    long totalRows;
    LongWritable key = null;

    public RangeRecordReader() {
    }

    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      startRow = ((RangeInputSplit) split).firstRow;
      finishedRows = 0;
      totalRows = ((RangeInputSplit) split).rowCount;
    }

    public void close() throws IOException {
      // NOTHING
    }

    public LongWritable getCurrentKey() {
      return key;
    }

    public NullWritable getCurrentValue() {
      return NullWritable.get();
    }

    public float getProgress() throws IOException {
      return finishedRows / (float) totalRows;
    }

    public boolean nextKeyValue() {
      if (key == null) {
        key = new LongWritable();
      }
      if (finishedRows < totalRows) {
        key.set(startRow + finishedRows);
        finishedRows += 1;
        return true;
      } else {
        return false;
      }
    }

  }

  public RecordReader<LongWritable, NullWritable>
  createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    return new RangeRecordReader();
  }

  static long getNumberOfRows(JobContext job) {
    return job.getConfiguration().getLong(Constants.NUM_ROWS, 0);
  }

  /**
   * Create the desired number of splits, dividing the number of rows
   * between the mappers.
   */
  public List<InputSplit> getSplits(JobContext job) {
    long totalRows = getNumberOfRows(job);
    int numSplits = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
    LOG.info("Generating " + totalRows + " using " + numSplits);
    List<InputSplit> splits = new ArrayList<InputSplit>();
    long currentRow = 0;
    for (int split = 0; split < numSplits; ++split) {
      long goal =
          (long) Math.ceil(totalRows * (double) (split + 1) / numSplits);
      splits.add(new RangeInputSplit(currentRow, goal - currentRow));
      currentRow = goal;
    }
    return splits;
  }

}