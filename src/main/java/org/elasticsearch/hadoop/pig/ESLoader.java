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
package org.elasticsearch.hadoop.pig;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.elasticsearch.hadoop.mr.ESConfigConstants;
import org.elasticsearch.hadoop.mr.ESInputFormat;
import org.elasticsearch.hadoop.util.ConfigUtils;
import org.elasticsearch.hadoop.util.WritableUtils;

/**
 * A loader for accessing and querying one or multiple ElasticSearch index/indices.
 * <p/>
 * Typical usage is:
 *
 * <pre>
 * A = LOAD 'twitter/_search?q=kimchy' USING org.elasticsearch.hadoop.pig.ESLoader();
 * </pre>
 *
 * The ElasticSearch host/port can be specified through Hadoop properties (see package description)
 * or passed to the {@link #ESLoader(String, int)} constructor.
 */
//TODO: hook maybe a user defined schema
public class ESLoader extends LoadFunc {

    private String relativeLocation = null;

    private final String host;
    private int port = 0;

    private RecordReader<Text, MapWritable> reader;

    public ESLoader() {
        this(null, 0);
    }

    public ESLoader(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        // TODO: add validation (no host/port or leading /)
        Configuration cfg = job.getConfiguration();
        cfg.set(ESConfigConstants.ES_ADDRESS, ConfigUtils.detectHostPortAddress(host, port, cfg));
        cfg.set(ESConfigConstants.ES_QUERY, location.trim());
    }

    @Override
    public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
        // TODO: potentially do additional parsing here
        relativeLocation = location;
        return relativeLocation;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public InputFormat getInputFormat() throws IOException {
        return new ESInputFormat();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (!reader.nextKeyValue()) {
                return null;
            }

            Tuple tuple = TupleFactory.getInstance().newTuple(2);
            tuple.set(0, WritableUtils.fromWritable(reader.getCurrentKey()));
            Object map = WritableUtils.fromWritable(reader.getCurrentValue());
            tuple.set(1, map);

            return tuple;

        } catch (InterruptedException ex) {
            throw new IOException("interrupted", ex);
        }
    }

    @Override
    public void setUDFContextSignature(String signature) {
        //TODO: double check backend execution
    }
}
