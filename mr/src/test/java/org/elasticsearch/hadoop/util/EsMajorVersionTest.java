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
package org.elasticsearch.hadoop.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.junit.Test;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EsMajorVersionTest {
    private static final List<String> TEST_VERSIONS;
    static {
        List<String> versions = new ArrayList<>();
        versions.add("0.1.0");
        versions.add("0.1.1");
        versions.add("1.0.0-alpha1");
        versions.add("1.0.0-beta2");
        versions.add("1.0.0-rc3");
        versions.add("1.0.0");
        versions.add("1.1.0");
        versions.add("1.1.1");
        versions.add("2.0.0-alpha1");
        versions.add("2.0.0-beta2");
        versions.add("2.0.0-rc3");
        versions.add("2.0.0");
        versions.add("2.1.0");
        versions.add("2.1.1");
        versions.add("5.0.0-alpha1");
        versions.add("5.0.0-beta2");
        versions.add("5.0.0-rc3");
        versions.add("5.0.0");
        versions.add("5.1.0");
        versions.add("5.1.1");
        versions.add("6.0.0-alpha1");
        versions.add("6.0.0-beta2");
        versions.add("6.0.0-rc3");
        versions.add("6.0.0");
        versions.add("6.1.0");
        versions.add("6.1.1");
        versions.add("7.0.0-alpha1");
        versions.add("7.0.0-beta2");
        versions.add("7.0.0-rc3");
        versions.add("7.0.0");
        versions.add("7.1.0");
        versions.add("7.1.1");
        versions.add("8.0.0-alpha1");
        versions.add("8.0.0-beta2");
        versions.add("8.0.0-rc3");
        versions.add("8.0.0");
        versions.add("8.1.0");
        versions.add("8.1.1");
        TEST_VERSIONS = Collections.unmodifiableList(versions);
    }


    @Test
    public void testVersionFromString() {
        for (int i = 0; i < TEST_VERSIONS.size(); i++) {
            String testVersion = TEST_VERSIONS.get(i);
            EsMajorVersion version = EsMajorVersion.parse(testVersion);
            EsMajorVersion version2 = EsMajorVersion.parse(testVersion);
            assertTrue(version.onOrAfter(version));
            assertTrue(version.equals(version));
            assertTrue(version.equals(version2));
            for (int j = i + 1; j < TEST_VERSIONS.size(); j++) {
                String laterTestVersion = TEST_VERSIONS.get(j);
                EsMajorVersion compareVersion = EsMajorVersion.parse(laterTestVersion);
                assertTrue(compareVersion.onOrAfter(version));
                assertFalse(compareVersion.equals(version));
            }

            for (int j = i - 1; j >= 0; j--) {
                String earlierTestVersion = TEST_VERSIONS.get(j);
                EsMajorVersion cmp_version = EsMajorVersion.parse(earlierTestVersion);
                assertTrue(cmp_version.onOrBefore(version));
                assertFalse(cmp_version.equals(version));
            }
        }
    }

    @Test
    public void testMinorVersionParsing() {
        for (String testVersion : TEST_VERSIONS) {
            EsMajorVersion version = EsMajorVersion.parse(testVersion);
            int minorVersion = version.parseMinorVersion(testVersion);
            assertThat(minorVersion, greaterThanOrEqualTo(0));
        }
        try {
            EsMajorVersion.V_7_X.parseMinorVersion("6.0.0");
            fail("Invalid major version");
        } catch (EsHadoopIllegalArgumentException e) {
            assertEquals("Invalid version string for major version; Received [6.0.0] for major version [7.x]",
                    e.getMessage());
        }
        try {
            EsMajorVersion.V_7_X.parseMinorVersion("7.");
            fail("Invalid major version");
        } catch (EsHadoopIllegalArgumentException e) {
            assertEquals("Could not parse Elasticsearch minor version [7.]. Invalid version format.",
                    e.getMessage());
        }
        try {
            EsMajorVersion.V_7_X.parseMinorVersion("7.4-abcd.4");
            fail("Invalid major version");
        } catch (EsHadoopIllegalArgumentException e) {
            assertEquals("Could not parse Elasticsearch minor version [7.4-abcd.4]. Non-numeric minor version [4-abcd].",
                    e.getMessage());
        }
    }
}