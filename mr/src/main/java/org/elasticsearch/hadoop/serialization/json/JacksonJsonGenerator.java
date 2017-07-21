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
package org.elasticsearch.hadoop.serialization.json;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.Deque;
import java.util.LinkedList;

import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException;
import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.util.StringUtils;

public class JacksonJsonGenerator implements Generator {

    private static final boolean HAS_UTF_8;
    private static final JsonFactory JSON_FACTORY;
    private final JsonGenerator generator;
    private final OutputStream out;
    private Deque<String> currentPath = new LinkedList<String>();
    private String currentPathCached;
    private String currentName;

    static {
        boolean hasMethod = false;
        try {
            Method m = JsonGenerator.class.getMethod("writeUTF8String", byte[].class, int.class, int.class);
            hasMethod = true;
        } catch (NoSuchMethodException ex) {
        }
        HAS_UTF_8 = hasMethod;
        if (!HAS_UTF_8) {
            LogFactory.getLog(JacksonJsonGenerator.class).warn(
                    "Old Jackson version (pre-1.7) detected; consider upgrading to improve performance");
        }

        JSON_FACTORY = new JsonFactory();
        JSON_FACTORY.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
    }

    public JacksonJsonGenerator(OutputStream out) {
        try {
            this.out = out;
            // use dedicated method to lower Jackson requirement
            this.generator = JSON_FACTORY.createJsonGenerator(out, JsonEncoding.UTF8);
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    public void usePrettyPrint() {
        generator.useDefaultPrettyPrinter();
    }

    @Override
    public Generator writeBeginArray() {
        try {
            generator.writeStartArray();
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeEndArray() {
        try {
            generator.writeEndArray();
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeBeginObject() {
        try {
            generator.writeStartObject();
            if (currentName != null) {
                currentPath.addLast(currentName);
                currentName = null;
                currentPathCached = null;
            }
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeEndObject() {
        try {
            generator.writeEndObject();
            currentName = currentPath.pollLast();
            currentPathCached = null;
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeFieldName(String name) {
        try {
            generator.writeFieldName(name);
            currentName = name;
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeString(String text) {
        try {
            generator.writeString(text);
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeUTF8String(byte[] text, int offset, int len) {
        try {
            if (HAS_UTF_8) {
                generator.writeUTF8String(text, offset, len);
            }
            else {
                generator.writeString(new String(text, offset, len, StringUtils.UTF_8));
            }
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeUTF8String(byte[] text) {
        return writeUTF8String(text, 0, text.length);
    }

    @Override
    public Generator writeBinary(byte[] data, int offset, int len) {
        try {
            generator.writeBinary(data, offset, len);
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeBinary(byte[] data) {
        return writeBinary(data, 0, data.length);
    }

    @Override
    public Generator writeNumber(short s) {
        return writeNumber((int) s);
    }

    @Override
    public Generator writeNumber(byte b) {
        return writeNumber((int) b);
    }

    @Override
    public Generator writeNumber(int i) {
        try {
            generator.writeNumber(i);
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeNumber(long l) {
        try {
            generator.writeNumber(l);
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeNumber(double d) {
        try {
            generator.writeNumber(d);
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeNumber(float f) {
        try {
            generator.writeNumber(f);
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeBoolean(boolean b) {
        try {
            generator.writeBoolean(b);
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeNull() {
        try {
            generator.writeNull();
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Generator writeRaw(String value) {
        try {
            generator.writeRaw(value);
            return this;
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public void flush() {
        try {
            generator.flush();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public void close() {
        try {
            generator.close();
        } catch (IOException ex) {
            throw new EsHadoopSerializationException(ex);
        }
    }

    @Override
    public Object getOutputTarget() {
        //return generator.getOutputTarget();
        return out;
    }

    @Override
    public String getParentPath() {
        if (currentPathCached == null) {
            if (currentPath.isEmpty()) {
                currentPathCached = StringUtils.EMPTY;
            }
            else {
                StringBuilder sb = new StringBuilder();
                for (String level : currentPath) {
                    sb.append(level);
                    sb.append(".");
                }
                sb.setLength(sb.length() - 1);
                currentPathCached = sb.toString();
            }
        }

        return currentPathCached;
    }
}