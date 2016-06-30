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

package org.elasticsearch.hadoop.util.unit;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class BooleansTest {

    @Parameters
    public static Collection<Object[]> parameters() {
        Collection<Object[]> params = new ArrayList<Object[]>();
        params.add(new Object[]{ "false", false });
        params.add(new Object[]{ "off", false });
        params.add(new Object[]{ "no", false });
        params.add(new Object[]{ "0", false });
        params.add(new Object[]{ "", false });

        params.add(new Object[]{ "true", true });
        params.add(new Object[]{ "on", true });
        params.add(new Object[]{ "yes", true });
        params.add(new Object[]{ "1", true });
        params.add(new Object[]{ "aggablagblag", true });

        return params;
    }

    private String input;
    private boolean expected;

    public BooleansTest(String input, boolean expected) {
        this.input = input;
        this.expected = expected;
    }

    @Test
    public void parseBoolean() throws Exception {
        Assert.assertEquals(String.format("Value was : [%s]", input), expected, Booleans.parseBoolean(input));
    }

}