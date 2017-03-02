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

package org.elasticsearch.hadoop.mr;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.elasticsearch.hadoop.util.BytesArray;

/**
 *
 */
class SafeWritableConverter {
    public SafeWritableConverter() {
        Text.class.getName(); // force class to be loaded
    }

    public void invoke(Object from, BytesArray to) {
        // handle common cases
        if (from instanceof Text) {
            Text t = (Text) from;
            to.bytes(t.getBytes(), t.getLength());
        }
        if (from instanceof BytesWritable) {
            BytesWritable b = (BytesWritable) from;
            to.bytes(b.getBytes(), b.getLength());
        }
    }
}
