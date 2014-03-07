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

import java.io.IOException;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;

/**
 * Wrapper class around a Pig tuple - the value and its associated schema.
 */
public class PigTuple {

    private Tuple tuple;
    private ResourceFieldSchema schemaField;

    public PigTuple(ResourceSchema schema) {
        setSchema(schema);
    }

    public Tuple getTuple() {
        return tuple;
    }

    public void setTuple(Tuple object) {
        this.tuple = object;
    }

    public ResourceFieldSchema getSchema() {
        return schemaField;
    }

    public void setSchema(ResourceSchema schema) {
        schemaField = new ResourceFieldSchema();
        schemaField.setType(DataType.TUPLE);
        try {
            schemaField.setSchema(schema);
        } catch (IOException ex) {
            throw new EsHadoopIllegalStateException(String.format("Cannot use schema [%s]", schema), ex);
        }
    }
}
