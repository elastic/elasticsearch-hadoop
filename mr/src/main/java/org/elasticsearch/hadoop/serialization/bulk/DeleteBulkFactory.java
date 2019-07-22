/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.serialization.bulk;

import org.apache.hadoop.io.*;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.StringUtils;

import java.util.List;

public class DeleteBulkFactory extends AbstractBulkFactory {

    public static final class NoDataWriter implements ValueWriter<Object> {

        @Override
        public Result write(Object writable, Generator generator) {
            ///delete doesn't require any content but it needs to extract metadata associated to a document
            if (writable == null || writable instanceof NullWritable) {
                generator.writeNull();
            }
            else if (writable instanceof Text) {
                Text text = (Text) writable;
                generator.writeUTF8String(text.getBytes(), 0, text.getLength());
            }
            else if (writable instanceof UTF8) {
                UTF8 utf8 = (UTF8) writable;
                generator.writeUTF8String(utf8.getBytes(), 0, utf8.getLength());
            }
            else if (writable instanceof IntWritable) {
                generator.writeNumber(((IntWritable) writable).get());
            }
            else if (writable instanceof LongWritable) {
                generator.writeNumber(((LongWritable) writable).get());
            }
            else if (writable instanceof VLongWritable) {
                generator.writeNumber(((VLongWritable) writable).get());
            }
            else if (writable instanceof VIntWritable) {
                generator.writeNumber(((VIntWritable) writable).get());
            }
            else if (writable instanceof ByteWritable) {
                generator.writeNumber(((ByteWritable) writable).get());
            }
            else if (writable instanceof DoubleWritable) {
                generator.writeNumber(((DoubleWritable) writable).get());
            }
            else if (writable instanceof FloatWritable) {
                generator.writeNumber(((FloatWritable) writable).get());
            }
            else if (writable instanceof BooleanWritable) {
                generator.writeBoolean(((BooleanWritable) writable).get());
            }
            else if (writable instanceof BytesWritable) {
                BytesWritable bw = (BytesWritable) writable;
                generator.writeBinary(bw.getBytes(), 0, bw.getLength());
            }
            else if (writable instanceof MD5Hash) {
                generator.writeString(writable.toString());
            }
            return Result.SUCCESFUL();
        }
    }

    public DeleteBulkFactory(Settings settings, MetadataExtractor metaExtractor, EsMajorVersion version) {
        // we only want a specific serializer for this particular bulk factory
        super(settings.copy().setSerializerValueWriterClassName(NoDataWriter.class.getName()), metaExtractor, version);
    }

    @Override
    protected String getOperation() {
        return ConfigurationOptions.ES_OPERATION_DELETE;
    }

    @Override
    protected void writeObjectEnd(List<Object> list) {
        // skip adding new-line for each entity as delete doesn't need entity output
        list.add(StringUtils.EMPTY);
    }
}
