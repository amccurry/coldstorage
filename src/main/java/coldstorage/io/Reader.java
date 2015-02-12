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

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.RandomAccess;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Reader {

  public static void main(String[] args) throws IOException {

    List<Long> idsToFind = new ArrayList<Long>();
    int maxId = 100000000;
    Random random = new Random(1);
    for (int i = 0; i < 1000; i++) {
      long id = (long) random.nextInt(maxId);
//      System.out.println(id);
      idsToFind.add(id);
    }

    // idsToFind.clear();
    // idsToFind.add(58998000L);

//    Path pathData = new Path("./out/data.avro");
//    Path pathIndex = new Path("./out/data.index");
    
    Path pathData = new Path("hdfs://localhost:9000/avro/out/data.avro");
    Path pathIndex = new Path("hdfs://localhost:9000/avro/out/data.index");
    
    Configuration configuration = new Configuration();
    FileSystem fileSystem = pathData.getFileSystem(configuration);
    FileStatus indexFileStatus = fileSystem.getFileStatus(pathIndex);
    FileStatus dataFileStatus = fileSystem.getFileStatus(pathData);
    FSDataInputStream indexInputStream = fileSystem.open(pathIndex);
    FSDataInputStream dataInputStream = fileSystem.open(pathData);

    AvroFSInput fsInput = new AvroFSInput(dataInputStream, dataFileStatus.getLen());
    GenericDatumReader<GenericRecord> gdr = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(fsInput, gdr);

    List<IndexKey> list = getList(indexInputStream, indexFileStatus.getLen());

    for (Long idToFind : idsToFind) {
      long t1 = System.nanoTime();
      GenericRecord lookupRecord = lookupRecord(reader, list, idToFind);
      long t2 = System.nanoTime();
      System.out.println("Found [" + idToFind + "] in [" + (t2 - t1) / 1000000.0 + " ms]:" + lookupRecord);
    }
  }

  private static GenericRecord lookupRecord(DataFileReader<GenericRecord> reader, List<IndexKey> list, Long idToFind)
      throws IOException {
    IndexKey lookup = new IndexKey(idToFind);
    int position = Collections.binarySearch(list, lookup);
    // System.out.println(position);
    IndexKey indexKey;
    if (position >= 0) {
      int index = position - 1;
      if (index < 0) {
        index = 0;
      }
      indexKey = list.get(index);
      // System.out.println("hit " + indexKey);
    } else {
      int index = Math.abs(position) - 2; // Get key before
      if (index < 0) {
        index = 0;
      }
      indexKey = list.get(index);
      // System.out.println("close " + indexKey);
    }

    long dataPosition;
    if (idToFind < indexKey.getId()) {
      dataPosition = 0;
    } else {
      dataPosition = indexKey.getPos();
    }
    
//    System.out.println(indexKey);

    reader.seek(dataPosition);

    GenericRecord genericRecord;
    while (reader.hasNext() && idIsNotGreaterThanLookupId(genericRecord = reader.next(), idToFind)) {
//      System.out.println("Checking [" + genericRecord + "]");
      if (isRecordCorrect(genericRecord, idToFind)) {
        return genericRecord;
      }
    }
    return null;
  }

  private static boolean isRecordCorrect(GenericRecord genericRecord, long idToFind) {
    long id = (Long) genericRecord.get("id");
    if (id == idToFind) {
      return true;
    }
    return false;
  }

  private static boolean idIsNotGreaterThanLookupId(GenericRecord genericRecord, long idToFind) {
    long id = (Long) genericRecord.get("id");
    if (id > idToFind) {
      return false;
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  private static List<IndexKey> getList(final FSDataInputStream reader, long length) {
    final int keyWidth = 8 * 2;
    final int size = (int) (length / keyWidth);
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("size")) {
          return size;
        } else if (method.getName().equals("get")) {
          int index = (Integer) args[0];
          long seek = index * (long) keyWidth;
          reader.seek(seek);
          IndexKey indexKey = new IndexKey();
          indexKey.readFields(reader);
          return indexKey;
        } else {
          throw new RuntimeException("Not supported.");
        }
      }
    };
    return (List<IndexKey>) Proxy.newProxyInstance(List.class.getClassLoader(), new Class[] { List.class,
        RandomAccess.class }, handler);
  }
}
