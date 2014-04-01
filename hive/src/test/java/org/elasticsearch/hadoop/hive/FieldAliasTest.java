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
package org.elasticsearch.hadoop.hive;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.util.FieldAlias;
import org.junit.Test;

import static org.junit.Assert.*;

public class FieldAliasTest {

    @Test
    public void testFieldMap() throws Exception {
        Properties tableProperties = new Properties();
        tableProperties.put(HiveConstants.MAPPING_NAMES, "timestamp:@timestamp , foo:123foo");
        FieldAlias alias = HiveUtils.alias(new PropertiesSettings(tableProperties));
        assertEquals("@timestamp", alias.toES("timestamp"));
        assertEquals("123foo", alias.toES("foo"));
        assertEquals("bar", alias.toES("BaR"));
    }

    @Test
    public void testFieldMapWithColumns() throws Exception {
        Properties tableProperties = new Properties();
        tableProperties.put(HiveConstants.MAPPING_NAMES, "timestamp:@timestamp , foo:123foo");
        tableProperties.put(HiveConstants.COLUMNS, "id,name,timestamp,foo");
        FieldAlias alias = HiveUtils.alias(new PropertiesSettings(tableProperties));
        assertEquals("@timestamp", alias.toES("timestamp"));
        assertEquals("123foo", alias.toES("foo"));
        assertEquals("bar", alias.toES("BaR"));
    }

    @Test
    public void testColumnToAlias() throws Exception {
        Properties tableProperties = new Properties();
        tableProperties.put(HiveConstants.MAPPING_NAMES, "timestamp:@timestamp , foo:123foo");
        tableProperties.put(HiveConstants.COLUMNS, "id,name,timestamp,foo");
        Collection<String> columnToAlias = HiveUtils.columnToAlias(new PropertiesSettings(tableProperties));
        assertEquals(4, columnToAlias.size());
        Iterator<String> iterator = columnToAlias.iterator();
        assertEquals("id", iterator.next());
        assertEquals("name", iterator.next());
        assertEquals("@timestamp", iterator.next());
        assertEquals("123foo", iterator.next());
    }
}
