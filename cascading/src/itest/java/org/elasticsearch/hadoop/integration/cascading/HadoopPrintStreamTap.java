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
package org.elasticsearch.hadoop.integration.cascading;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.elasticsearch.hadoop.Stream;
import org.elasticsearch.hadoop.util.WritableUtils;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.SinkMode;
import cascading.tap.SinkTap;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntrySchemeCollector;

class HadoopPrintStreamTap extends SinkTap<JobConf, Object> implements Serializable {

    /** */
    private static final long serialVersionUID = 1L;

    private final Stream stream;

    private static class SysoutScheme extends Scheme<JobConf, Object, Object, Object, Object> implements Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, Object, Object> tap, JobConf conf) {
            // no-op
        }

        @Override
        public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, Object, Object> tap, JobConf conf) {
            // no-op
        }

        @Override
        public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object, Object> sourceCall) throws IOException {
            // no-op
            return false;
        }

        @Override
        public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object, Object> sinkCall) throws IOException {
            Tuple tuple = sinkCall.getOutgoingEntry().getTuple();
            StringBuffer sb = new StringBuffer();
            for (Object object : tuple) {
                if (object instanceof Writable) {
                    sb.append(WritableUtils.fromWritable((Writable) object));
                }
                else {
                    sb.append(object);
                }
                sb.append(" ");
            }
            ((PrintStream) sinkCall.getOutput()).println(sb.toString());
        }
    }

    public HadoopPrintStreamTap() {
        this(Stream.OUT);
    }

    public HadoopPrintStreamTap(Stream stream) {
        super(new SysoutScheme(), SinkMode.UPDATE);
        this.stream = stream;
    }

    @Override
    public String getIdentifier() {
        return "HadoopStdOut";
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess, Object output) throws IOException {
        return new TupleEntrySchemeCollector(flowProcess, getScheme(), stream.stream());
    }

    @Override
    public boolean createResource(JobConf conf) throws IOException {
        return true;
    }

    @Override
    public boolean deleteResource(JobConf conf) throws IOException {
        return false;
    }

    @Override
    public boolean resourceExists(JobConf conf) throws IOException {
        return true;
    }

    @Override
    public long getModifiedTime(JobConf conf) throws IOException {
        return 0;
    }
}
