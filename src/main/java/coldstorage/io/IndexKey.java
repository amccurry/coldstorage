/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package coldstorage.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IndexKey implements WritableComparable<IndexKey> {

  private long id;
  private long pos;

  public IndexKey() {

  }

  public IndexKey(long id) {
    this.id = id;
    this.pos = -1l;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(id);
    out.writeLong(pos);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readLong();
    pos = in.readLong();
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getPos() {
    return pos;
  }

  public void setPos(long pos) {
    this.pos = pos;
  }

  @Override
  public int compareTo(IndexKey o) {
    long diff = id - o.id;
    if (diff == 0) {
      return 0;
    }
    return diff < 0 ? -1 : 1;
  }

  @Override
  public String toString() {
    return "IndexKey [id=" + id + ", pos=" + pos + "]";
  }

}
