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
package org.elasticsearch.hadoop.rest;

import java.io.IOException;
import java.io.InputStream;

import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;

public class DelegatingInputStream extends InputStream implements StatsAware {

    private final InputStream delegate;
    private final Stats stats = new Stats();

    public DelegatingInputStream(InputStream delegate) {
        this.delegate = delegate;
    }

    public int read() throws IOException {
        return delegate.read();
    }

    public int hashCode() {
        return delegate.hashCode();
    }

    public int read(byte[] b) throws IOException {
        int result = delegate.read(b);
        if (result > 0) {
            stats.bytesRead++;
        }
        return result;
    }

    public boolean equals(Object obj) {
        return delegate.equals(obj);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        int result = delegate.read(b, off, len);
        if (result > 0) {
            stats.bytesRead += result;
        }
        return result;
    }

    public long skip(long n) throws IOException {
        return delegate.skip(n);
    }

    public int available() throws IOException {
        return delegate.available();
    }

    public String toString() {
        return delegate.toString();
    }

    public void close() throws IOException {
        delegate.close();
    }

    public void mark(int readlimit) {
        delegate.mark(readlimit);
    }

    public void reset() throws IOException {
        delegate.reset();
    }

    public boolean markSupported() {
        return delegate.markSupported();
    }

    public boolean isNull() {
        return delegate == null;
    }

    @Override
    public Stats stats() {
        return stats;
    }
}