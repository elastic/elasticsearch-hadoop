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
package org.elasticsearch.hadoop.pig;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.serialization.JdkBytesConverter;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;

public class PigBytesConverter extends JdkBytesConverter {

    @Override
    public void convert(Object from, BytesArray to) {

        // expect PigTuple holding a Tuple with only one field - chararray or bytearray
        Assert.isTrue(from instanceof PigTuple,
                String.format("Unexpected object type, expecting [%s], given [%s]", PigTuple.class, from.getClass()));

        PigTuple pt = (PigTuple) from;
        ResourceFieldSchema schema = pt.getSchema();

        // unwrap the tuple
        ResourceSchema tupleSchema = schema.getSchema();

        // empty tuple shortcut
        if (tupleSchema == null) {
            // write empty doc
            to.bytes("{}");
            return;
        }

        ResourceFieldSchema[] fields = tupleSchema.getFields();
        Assert.isTrue(fields.length == 1, "When using JSON input, only one field is expected");

        Object object;
        byte type;

        try {
            object = pt.getTuple().get(0);
            type = pt.getTuple().getType(0);
        } catch (Exception ex) {
            throw new EsHadoopIllegalStateException("Encountered exception while processing tuple", ex);
        }


        if (type == DataType.BIGCHARARRAY || type == DataType.CHARARRAY) {
            to.bytes(object.toString());
            return;
        }
        if (type == DataType.BYTEARRAY) {
            DataByteArray dba = (DataByteArray) object;
            to.bytes(dba.get(), dba.size());
            return;
        }

        throw new EsHadoopIllegalArgumentException(String.format("Cannot handle Pig type [%s]; expecting [%s,%s]", object.getClass(), String.class, DataByteArray.class));
    }
}
