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
package org.elasticsearch.hadoop.cascading;

import org.elasticsearch.hadoop.mr.WritableBytesConverter;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;

import cascading.scheme.SinkCall;
import cascading.tuple.Tuple;

public class CascadingLocalBytesConverter extends WritableBytesConverter {

    @Override
    public void convert(Object from, BytesArray to) {
        // expect a tuple holding one field - chararray or bytearray
        Assert.isTrue(from instanceof SinkCall,
                String.format("Unexpected object type, expecting [%s], given [%s]", SinkCall.class, from.getClass()));

        // handle common cases
        SinkCall sinkCall = (SinkCall) from;
        Tuple rawTuple = sinkCall.getOutgoingEntry().getTuple();

        if (rawTuple == null || rawTuple.isEmpty()) {
            to.bytes("{}");
            return;
        }
        Assert.isTrue(rawTuple.size() == 1, "When using JSON input, only one field is expected");

        // postpone the coercion
        Tuple tuple = CascadingUtils.coerceToString(sinkCall);
        to.bytes(tuple.getObject(0).toString());
        return;
    }
}
