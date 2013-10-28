/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.rest;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;

import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.StringUtils;

enum Operation {

    CREATE {
        private final byte[] OP = ("{\"" + ConfigurationOptions.ES_OPERATION_CREATE + "\":{}}\n").getBytes(StringUtils.UTF_8);
        private final byte[] ID_PREFIX = ("{\"" + ConfigurationOptions.ES_OPERATION_CREATE + "\":{\"_id\":\"").getBytes(StringUtils.UTF_8);
        private final byte[] ID_SUFFIX = ("\"}}\n").getBytes(StringUtils.UTF_8);

        @Override
        void writeHeader(String id, BytesArray bytes) {
            if (StringUtils.hasText(id)) {
                copyIntoBuffer(ID_PREFIX, bytes);
                copyIntoBuffer(id.getBytes(StringUtils.UTF_8), bytes);
                copyIntoBuffer(ID_SUFFIX, bytes);
            } else {
                copyIntoBuffer(OP, bytes);
            }
        }

        @Override
        int jsonSize(String id) {
            if (id == null) {
                return OP.length;
            }
            return (id.length() * 2) + ID_PREFIX.length + ID_SUFFIX.length;
        }
    },

    INDEX {
        private final byte[] OP = ("{\"" + ConfigurationOptions.ES_OPERATION_INDEX + "\":{}}\n").getBytes(StringUtils.UTF_8);
        private final byte[] ID_PREFIX = ("{\"" + ConfigurationOptions.ES_OPERATION_INDEX + "\":{\"_id\":\"").getBytes(StringUtils.UTF_8);
        private final byte[] ID_SUFFIX = ("\"}}\n").getBytes(StringUtils.UTF_8);

        @Override
        void writeHeader(String id, BytesArray bytes) {
            if (StringUtils.hasText(id)) {
                copyIntoBuffer(ID_PREFIX, bytes);
                copyIntoBuffer(id.getBytes(StringUtils.UTF_8), bytes);
                copyIntoBuffer(ID_SUFFIX, bytes);
            } else {
                copyIntoBuffer(OP, bytes);
            }
        }

        @Override
        int jsonSize(String id) {
            if (id == null) {
                return OP.length;
            }
            return (id.length() * 2) + ID_PREFIX.length + ID_SUFFIX.length;
        }
    };

    private static final void copyIntoBuffer(byte[] content, BytesArray bytes) {
        System.arraycopy(content, 0, bytes.bytes(), bytes.size(), content.length);
        bytes.increment(content.length);
    }

    abstract void writeHeader(String id, BytesArray bytes);
    abstract int jsonSize(String id);

    private static final Map<String, Operation> enumMap = new LinkedHashMap<String, Operation>();

    static {
        for (Operation operation : EnumSet.allOf(Operation.class)) {
            enumMap.put(operation.name(), operation);
        }
    }

    static Operation fromString(String name) {
        if (StringUtils.hasText(name)) {
            Operation op = enumMap.get(name.toUpperCase());
            Assert.notNull(op, String.format("Unknown operation %s", name));
        }
        return enumMap.get(ConfigurationOptions.ES_OPERATION_DEFAULT.toUpperCase());
    }
}