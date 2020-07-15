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

package org.elasticsearch.spark.sql.streaming.api.java;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.elasticsearch.spark.sql.streaming.StreamingQueryTestHarness;
import scala.Function0;
import scala.runtime.AbstractFunction0;

/**
 * Java based wrapper around the scala base streaming query test harness
 */
public class JavaStreamingQueryTestHarness<S extends Serializable> {

    private final StreamingQueryTestHarness<S> harness;

    public JavaStreamingQueryTestHarness(SparkSession spark, Encoder<S> encoder) {
        this.harness = new StreamingQueryTestHarness<>(spark, encoder);
    }

    public Dataset<S> stream() {
        return harness.stream();
    }

    public JavaStreamingQueryTestHarness<S> withInput(S data) {
        harness.withInput(data);
        return this;
    }

    public JavaStreamingQueryTestHarness<S> setTestTimeout(long timeout, TimeUnit unit) {
        harness.setTestTimeout(timeout, unit);
        return this;
    }

    public JavaStreamingQueryTestHarness<S> expectingToThrow(Class<? extends Throwable> clazz) {
        harness.expectingToThrow(clazz);
        return this;
    }

    public JavaStreamingQueryTestHarness<S> expectingToThrow(Class<? extends Throwable> clazz, String message) {
        harness.expectingToThrow(clazz, message);
        return this;
    }

    public StreamingQuery start(final DataStreamWriter<?> writer, final String path) {
        Function0<StreamingQuery> runFunction = new AbstractFunction0<StreamingQuery>() {
            @Override
            public StreamingQuery apply() {
                return writer.start(path);
            }
        };
        return harness.startTest(runFunction);
    }

    public StreamingQuery start(final DataStreamWriter<?> writer) {
        Function0<StreamingQuery> runFunction = new AbstractFunction0<StreamingQuery>() {
            @Override
            public StreamingQuery apply() {
                return writer.start();
            }
        };
        return harness.startTest(runFunction);
    }

    public void run(final DataStreamWriter<?> writer, final String path) {
        Function0<StreamingQuery> runFunction = new AbstractFunction0<StreamingQuery>() {
            @Override
            public StreamingQuery apply() {
                return writer.start(path);
            }
        };
        harness.runTest(runFunction);
    }

    public void run(final DataStreamWriter<?> writer) {
        Function0<StreamingQuery> runFunction = new AbstractFunction0<StreamingQuery>() {
            @Override
            public StreamingQuery apply() {
                return writer.start();
            }
        };
        harness.runTest(runFunction);
    }

    public void waitForCompletion() {
        harness.waitForCompletion();
    }
}
