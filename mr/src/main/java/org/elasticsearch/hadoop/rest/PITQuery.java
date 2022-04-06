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

import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.serialization.PITReader;
import org.elasticsearch.hadoop.util.BytesArray;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * Result streaming data from a ElasticSearch query using the a PIT query. Performs batching underneath to retrieve data in chunks.
 */
public class PITQuery implements Iterator<Object>, Closeable, StatsAware {

    private RestRepository repository;
    private List<Object[]> batch = Collections.emptyList();
    private boolean finished = false;

    private int batchIndex = 0;
    private long read = 0;
    // how many docs to read - in most cases, all the docs that match
    private long size;

    private final PITReader reader;

    private final Stats stats = new Stats();

    private boolean closed = false;
    private boolean initialized = false;
    private String query;
    private Function<String, BytesArray> bodyGenerator;
    private String lastReadId = null;

    PITQuery(RestRepository client, String query, Function<String, BytesArray> bodyGenerator, long size, PITReader reader) {
        this.repository = client;
        this.size = size;
        this.reader = reader;
        this.query = query;
        this.bodyGenerator = bodyGenerator;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            finished = true;
            batch = Collections.emptyList();
            reader.close();
            // however we're closing it either way
//            if (StringUtils.hasText(pit)) {  //TODO
//                repository.getRestClient().deletePointInTime(pit);
//            }
            repository.close();
        }
    }

    @Override
    public boolean hasNext() {
        if (finished) {
            return false;
        }
// TODO: Use slices
        if (!initialized) {
            initialized = true;

            try {
                PITReader.PIT pit = repository.pitQuery(query, bodyGenerator.apply(null), reader);
                if (pit == null) {
                    finished = true;
                    return false;
                }
                // size is passed as a limit (since we can't pass it directly into the request) - if it's not specified (<1) just query the
                // whole index
                size = (size < 1 ? pit.getTotalHits() : size);
                batch = pit.getHits();
                finished = pit.isConcluded();
                List<Object> previousLastSortValues = pit.getPreviousLastSortValues();
                if (previousLastSortValues != null && pit.getPreviousLastSortValues().isEmpty() == false) {
                    lastReadId = previousLastSortValues.get(0).toString();
                }
            } catch (IOException ex) {
                throw new EsHadoopIllegalStateException(String.format("Cannot query [%s/%s]", query, bodyGenerator.apply(null)), ex);
            }
            read += batch.size();
            stats.docsReceived += batch.size();
        }

        while (!finished && (batch.isEmpty() || batchIndex >= batch.size())) {
            if (read >= size) {
                finished = true;
                return false;
            }

            try {
                PITReader.PIT pit = repository.pitQuery(query, bodyGenerator.apply(lastReadId), reader);

                if (pit == null) {
                    finished = true;
                    return false;
                }
                List<Object> previousLastSortValues = pit.getPreviousLastSortValues();
                if (previousLastSortValues != null && pit.getPreviousLastSortValues().isEmpty() == false) {
                    lastReadId = previousLastSortValues.get(0).toString();
                }
                batch = pit.getHits();
                finished = pit.isConcluded();
            } catch (IOException ex) {
                throw new EsHadoopIllegalStateException("Error querying Elasticsearch", ex);
            }
            read += batch.size();
            stats.docsReceived += batch.size();

            // reset index
            batchIndex = 0;
        }

        return !finished;
    }

    public long getSize() {
        return size;
    }

    public long getRead() {
        return read;
    }

    @Override
    public Object[] next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more documents available");
        }
        return batch.get(batchIndex++);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("read-only operator");
    }

    @Override
    public Stats stats() {
        // there's no need to do aggregation
        return new Stats(stats);
    }

    public RestRepository repository() {
        return repository;
    }

}