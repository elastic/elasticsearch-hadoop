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

import java.util.List;

import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor;
import org.elasticsearch.hadoop.util.Assert;

public class PigFieldExtractor extends ConstantFieldExtractor {

    @Override
    protected Object extractField(Object target) {
        List<String> fieldNames = getFieldNames();
        for (int index = 0; index < fieldNames.size(); index++) {
            String fieldName = fieldNames.get(index);
            if (target instanceof PigTuple) {
                PigTuple pt = (PigTuple) target;
                ResourceFieldSchema[] fields = pt.getSchema().getSchema().getFields();

                boolean foundField = false;
                for (int i = 0; i < fields.length && !foundField; i++) {
                    ResourceFieldSchema field = fields[i];
                    if (fieldName.equals(field.getName())) {
                        foundField = true;
                        byte type = field.getType();
                        try {
                            Object object = pt.getTuple().get(i);
                            if (DataType.isAtomic(type)) {
                                target = object.toString();
                            }
                            else if (type == DataType.TUPLE) {
                                PigTuple rpt = new PigTuple(field.getSchema());
                                if (object instanceof PigTuple) {
                                    rpt.setTuple(((PigTuple) object).getTuple());
                                }
                                else {
                                    rpt.setTuple((Tuple) object);
                                }
                                target = rpt;
                            }
                            else {
                                Assert.isTrue(false, String.format("Unsupported data type [%s] for field [%s]; use only 'primitives' or 'tuples'", DataType.findTypeName(type), fieldName));
                            }
                        } catch (ExecException ex) {
                            throw new EsHadoopIllegalStateException(String.format("Cannot retrieve field [%s]", fieldName), ex);
                        }
                    }
                }
            }
            else {
                return NOT_FOUND;
            }
        }
        return target;
    }
}
