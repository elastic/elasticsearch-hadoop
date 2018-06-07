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

package org.elasticsearch.hadoop.cfg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.junit.Test;

import static org.junit.Assert.*;

public class CompositeSettingsTest {

    @Test
    public void testGetProperty() {
        Settings set1 = new PropertiesSettings();
        set1.setProperty("test.prop.one", "val1");

        Settings set2 = new PropertiesSettings();
        set2.setProperty("test.prop.one", "val2");
        set2.setProperty("test.prop.two", "val2");

        Settings set3 = new PropertiesSettings();
        set3.setProperty("test.prop.one", "val3");
        set3.setProperty("test.prop.two", "val3");
        set3.setProperty("test.prop.three", "val3");

        Settings composite = new CompositeSettings(Arrays.asList(set1, set2, set3));

        assertEquals("val1", composite.getProperty("test.prop.one"));
        assertEquals("val2", composite.getProperty("test.prop.two"));
        assertEquals("val3", composite.getProperty("test.prop.three"));
    }

    @Test
    public void testSetProperty() {
        Settings set1 = new PropertiesSettings();
        set1.setProperty("test.prop.one", "val1");

        Settings set2 = new PropertiesSettings();
        set2.setProperty("test.prop.one", "val2");
        set2.setProperty("test.prop.two", "val2");

        Settings set3 = new PropertiesSettings();
        set3.setProperty("test.prop.one", "val3");
        set3.setProperty("test.prop.two", "val3");
        set3.setProperty("test.prop.three", "val3");

        Settings composite = new CompositeSettings(Arrays.asList(set1, set2, set3));
        composite.setProperty("test.prop.zero", "val0");

        assertEquals("val0", composite.getProperty("test.prop.zero"));
        assertEquals("val1", composite.getProperty("test.prop.one"));
        assertEquals("val2", composite.getProperty("test.prop.two"));
        assertEquals("val3", composite.getProperty("test.prop.three"));

        assertNull("Subconfigs contaminated", set1.getProperty("test.prop.zero"));
        assertNull("Subconfigs contaminated", set2.getProperty("test.prop.zero"));
        assertNull("Subconfigs contaminated", set3.getProperty("test.prop.zero"));
    }

    @Test
    public void testCopy() {
        Settings set1 = new PropertiesSettings();
        set1.setProperty("test.prop.one", "val1");

        Settings set2 = new PropertiesSettings();
        set2.setProperty("test.prop.one", "val2");
        set2.setProperty("test.prop.two", "val2");

        Settings set3 = new PropertiesSettings();
        set3.setProperty("test.prop.one", "val3");
        set3.setProperty("test.prop.two", "val3");
        set3.setProperty("test.prop.three", "val3");

        Settings composite = new CompositeSettings(Arrays.asList(set1, set2, set3));
        composite.setProperty("test.prop.zero", "val0");

        Settings clonedComposite = composite.copy();

        composite.setProperty("test.prop.zero", "nval0");
        set1.setProperty("test.prop.one", "nval1");
        set2.setProperty("test.prop.two", "nval2");
        set3.setProperty("test.prop.three", "nval3");

        assertEquals("val0", clonedComposite.getProperty("test.prop.zero"));
        assertEquals("val1", clonedComposite.getProperty("test.prop.one"));
        assertEquals("val2", clonedComposite.getProperty("test.prop.two"));
        assertEquals("val3", clonedComposite.getProperty("test.prop.three"));

        assertEquals("nval0", composite.getProperty("test.prop.zero"));
        assertEquals("nval1", composite.getProperty("test.prop.one"));
        assertEquals("nval2", composite.getProperty("test.prop.two"));
        assertEquals("nval3", composite.getProperty("test.prop.three"));
    }

    @Test
    public void testAsProperties() {
        Settings set1 = new PropertiesSettings();
        set1.setProperty("test.prop.one", "val1");

        Settings set2 = new PropertiesSettings();
        set2.setProperty("test.prop.one", "val2");
        set2.setProperty("test.prop.two", "val2");

        Settings set3 = new PropertiesSettings();
        set3.setProperty("test.prop.one", "val3");
        set3.setProperty("test.prop.two", "val3");
        set3.setProperty("test.prop.three", "val3");

        Settings composite = new CompositeSettings(Arrays.asList(set1, set2, set3));
        composite.setProperty("test.prop.zero", "val0");

        Properties props = composite.asProperties();

        composite.setProperty("test.prop.zero", "nval0");
        set1.setProperty("test.prop.one", "nval1");
        set2.setProperty("test.prop.two", "nval2");
        set3.setProperty("test.prop.three", "nval3");

        assertEquals("val0", props.getProperty("test.prop.zero"));
        assertEquals("val1", props.getProperty("test.prop.one"));
        assertEquals("val2", props.getProperty("test.prop.two"));
        assertEquals("val3", props.getProperty("test.prop.three"));

        assertEquals("nval0", composite.getProperty("test.prop.zero"));
        assertEquals("nval1", composite.getProperty("test.prop.one"));
        assertEquals("nval2", composite.getProperty("test.prop.two"));
        assertEquals("nval3", composite.getProperty("test.prop.three"));
    }
}