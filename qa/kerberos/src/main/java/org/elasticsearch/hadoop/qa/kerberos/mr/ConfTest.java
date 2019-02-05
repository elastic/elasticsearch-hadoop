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

package org.elasticsearch.hadoop.qa.kerberos.mr;

import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConfTest extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ConfTest(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println("Args:");
        for (String arg : args) {
            System.out.println(arg);
        }
        System.out.println();
        System.out.println("Configuration:");
        for (Map.Entry<String, String> next : getConf()) {
            System.out.println(next.getKey() + ": " + next.getValue() + " (" + Arrays.toString(getConf().getPropertySources(next.getKey())) + ")");
        }
        System.out.println();
        return 0;
    }
}
