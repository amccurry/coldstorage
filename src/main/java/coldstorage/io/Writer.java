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
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Writer {

  public static void main(String[] args) throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse("{" + "\"namespace\": \"example.avro\", " + "\"type\": \"record\", "
        + "\"name\": \"User\", " + "\"fields\": [" + "     {\"name\": \"id\", \"type\": \"long\"},"
        + "     {\"name\": \"data\", \"type\": \"string\"}" + " ]}");

    GenericDatumWriter<GenericRecord> gdw = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dfw = new DataFileWriter<GenericRecord>(gdw);
    // Path pathData = new Path("./out/data.avro");
    // Path pathIndex = new Path("./out/data.index");

    Path pathData = new Path("hdfs://localhost:9000/avro/out/data.avro");
    Path pathIndex = new Path("hdfs://localhost:9000/avro/out/data.index");

    Configuration configuration = new Configuration();
    FileSystem fileSystem = pathData.getFileSystem(configuration);

    FSDataOutputStream indexOutputStream = fileSystem.create(pathIndex);
    FSDataOutputStream outputStream = fileSystem.create(pathData);
    dfw.create(schema, outputStream);
    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
    Random random = new Random(1);
    final int syncPoint = 1000;
    int count = 0;

    for (int i = 0; i < 100000000; i++) {
      genericRecordBuilder.set("id", (long) i);
      genericRecordBuilder.set("data", Long.toString(random.nextLong()));
      Record record = genericRecordBuilder.build();
      dfw.append(record);
      if (count >= syncPoint) {
        long sync = dfw.sync();
        Object object = record.get("id");
        writeIndex(indexOutputStream, sync, object);
        count = 0;
      }
      count++;
    }
    indexOutputStream.close();
    dfw.close();
  }

  private static void writeIndex(FSDataOutputStream indexOutputStream, long pos, Object object) throws IOException {
    if (object instanceof Long) {
      long id = (Long) object;
      indexOutputStream.writeLong(id);
      indexOutputStream.writeLong(pos);
    } else {
      throw new RuntimeException("not supported.");
    }
  }
}
