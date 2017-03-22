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
package org.elasticsearch.hadoop.serialization;

import java.io.Closeable;


public interface Generator extends Closeable {

    Generator writeBeginArray();

    Generator writeEndArray();

    Generator writeBeginObject();

    Generator writeEndObject();

    Generator writeFieldName(String name);

    Generator writeString(String text);

    Generator writeUTF8String(byte[] text, int offset, int len);

    Generator writeUTF8String(byte[] text);

    Generator writeBinary(byte[] data, int offset, int len);

    Generator writeBinary(byte[] data);

    Generator writeNumber(short s);

    Generator writeNumber(byte b);

    Generator writeNumber(int i);

    Generator writeNumber(long l);

    Generator writeNumber(double d);

    Generator writeNumber(float f);

    Generator writeBoolean(boolean b);

    Generator writeNull();

    Generator writeRaw(String value);

    void flush();

    void close();

    Object getOutputTarget();

    String getParentPath();
}
