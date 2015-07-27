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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import java.util.ArrayList;

public class TextSampler implements IndexedSortable {
  private ArrayList<Text> records = new ArrayList<Text>();

  public int compare(int i, int j) {
    Text left = records.get(i);
    Text right = records.get(j);
    return left.compareTo(right);
  }

  public void swap(int i, int j) {
    Text left = records.get(i);
    Text right = records.get(j);
    records.set(j, left);
    records.set(i, right);
  }

  public void addKey(Text key) {
    synchronized (this) {
      records.add(new Text(key));
    }
  }

  /**
   * Find the split points for a given sample. The sample keys are sorted
   * and down sampled to find even split points for the partitions. The
   * returned keys should be the start of their respective partitions.
   *
   * @param numPartitions the desired number of partitions
   * @return an array of size numPartitions - 1 that holds the split points
   */
  public Text[] createPartitions(int numPartitions) {
    int numRecords = records.size();
    System.out.println("Making " + numPartitions + " from " + numRecords +
        " sampled records");
    if (numPartitions > numRecords) {
      throw new IllegalArgumentException
          ("Requested more partitions than input keys (" + numPartitions +
              " > " + numRecords + ")");
    }
    new QuickSort().sort(this, 0, records.size());
    float stepSize = numRecords / (float) numPartitions;
    Text[] result = new Text[numPartitions - 1];
    for (int i = 1; i < numPartitions; ++i) {
      result[i - 1] = records.get(Math.round(stepSize * i));
    }
    return result;
  }
}