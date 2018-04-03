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
package org.elasticsearch.hadoop.serialization.builder;

import java.util.Deque;
import java.util.LinkedList;

import org.elasticsearch.hadoop.EsHadoopIllegalStateException;

/**
 * A base implementation of a value reader that keeps track of which field is the current field being read.
 */
public abstract class AbstractValueReader implements ValueReader {

    /**
     * Encapsulates most of the field specific information that should be persisted when beginning to parse
     * a nested field.
     */
    public static class FieldContext {
        private String fieldName;
        private int arrayDepth;

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public int getArrayDepth() {
            return arrayDepth;
        }

        public void setArrayDepth(int arrayDepth) {
            this.arrayDepth = arrayDepth;
        }
    }

    private final Deque<FieldContext> nestedFieldContexts = new LinkedList<FieldContext>();

    /**
     * @return The information about the current field, or null if not set.
     */
    protected FieldContext getCurrentField() {
        return nestedFieldContexts.peek();
    }

    @Override
    public void beginField(String fieldName) {
        FieldContext fieldContext = new FieldContext();
        fieldContext.setFieldName(fieldName);
        fieldContext.setArrayDepth(0);
        nestedFieldContexts.push(fieldContext);
    }

    @Override
    public void endField(String fieldName) {
        FieldContext ctx = nestedFieldContexts.pop();
        if (ctx == null) {
            throw new EsHadoopIllegalStateException("Trying to end parsing of field [" + fieldName + "] but no field has been marked to begin parsing.");
        }
        if (!ctx.fieldName.equals(fieldName)) {
            throw new EsHadoopIllegalStateException("Trying to end parsing of field [" + fieldName + "] but the current field [" + ctx.fieldName + "] is being parsed.");
        }
    }
}
