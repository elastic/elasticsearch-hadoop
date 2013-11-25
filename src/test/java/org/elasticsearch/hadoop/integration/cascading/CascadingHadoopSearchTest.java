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
package org.elasticsearch.hadoop.integration.cascading;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.cascading.ESTap;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.integration.HdpBootstrap;
import org.elasticsearch.hadoop.integration.QueryTestParams;
import org.elasticsearch.hadoop.integration.Stream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Count;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@RunWith(Parameterized.class)
public class CascadingHadoopSearchTest {

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.params();
    }

    private String query;

    public CascadingHadoopSearchTest(String query) {
        this.query = query;
    }

    public static class FieldExtractor extends BaseOperation implements Function {
        public FieldExtractor(Fields fieldDeclaration) {
            super(2, fieldDeclaration);
        }

        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            TupleEntry argument = functionCall.getArguments();
            String doc_id = argument.getString(0);
            MapWritable map = (MapWritable) argument.getObject(1);

            Tuple result = new Tuple();
            result.add(doc_id);

            if (getFieldDeclaration().size() > 1) {
                for (int i = 0; i < getFieldDeclaration().size(); i++) {
                    Comparable field = getFieldDeclaration().get(i);
                    for (Map.Entry<Writable, Writable> s : map.entrySet()) {
                        if (field.compareTo(s.getKey().toString()) == 0) {
                            result.add(s.getValue());
                            break;
                        }
                    }
                }
            }

            functionCall.getOutputCollector().add(result);
        }
    }

    @Test
    public void testReadFromES() throws Exception {
        Tap in = new ESTap("cascading-hadoop/artists");
        Pipe pipe = new Pipe("copy");
        // print out
        Tap out = new HadoopPrintStreamTap(Stream.NULL);
        //Tap out = new Hfs(new TextDelimited(), "cascadingbug-1", SinkMode.REPLACE);
        FlowDef flowDef = FlowDef.flowDef().addSource(pipe, in).addTailSink(pipe, out);

        new HadoopFlowConnector(cfg()).connect(in, out, pipe).complete();
    }


    //@Test
    public void testCountFromES() throws Exception {
        Fields args = new Fields("doc_id", "description");
        Tap in = new ESTap("cascading-hadoop/artists", args);
        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, new Fields("doc_id", "description"), new FieldExtractor(args), Fields.RESULTS);
        pipe = new GroupBy(pipe);
        pipe = new Every(pipe, new Count());

        // print out
        Tap out = new HadoopPrintStreamTap(Stream.NULL);
        //Tap out = new Hfs(new TextDelimited(), "cascadingbug-2", SinkMode.REPLACE);
        FlowDef flowDef = FlowDef.flowDef().addSource(pipe, in).addTailSink(pipe, out);
        new HadoopFlowConnector(cfg()).connect(flowDef).complete();
    }

    private Properties cfg() {
        Properties props = HdpBootstrap.asProperties(QueryTestParams.provisionQueries(CascadingHadoopSuite.configuration));
        props.put(ConfigurationOptions.ES_QUERY, query);

        return props;
    }
}