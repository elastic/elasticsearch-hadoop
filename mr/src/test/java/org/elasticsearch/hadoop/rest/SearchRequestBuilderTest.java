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

import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.encoding.HttpEncodingTools;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class SearchRequestBuilderTest {

    @Test
    public void testVersion() {
        EsMajorVersion esVersion = EsMajorVersion.LATEST;
        SearchRequestBuilder includeVersionBuilder = new SearchRequestBuilder(esVersion, true);
        SearchRequestBuilder noVersionBuilder = new SearchRequestBuilder(esVersion, false);

        String versionQueryParam = "version=true";

        assertTrue(includeVersionBuilder.toString().contains(versionQueryParam));
        assertTrue(!noVersionBuilder.toString().contains(versionQueryParam));
    }

    @Test
    public void testPreference() {
        String preferenceString = "_only_nodes:abc*";
        String encodedPreferenceString = HttpEncodingTools.encode(preferenceString);

        EsMajorVersion esVersion = EsMajorVersion.LATEST;
        SearchRequestBuilder localOnlyBuilder = new SearchRequestBuilder(esVersion, true)
                .local(true);
        SearchRequestBuilder preferenceOnlyBuilder = new SearchRequestBuilder(esVersion, true)
                .preference(preferenceString);
        SearchRequestBuilder localWithPreferenceBuilder = new SearchRequestBuilder(esVersion, true)
                .local(true)
                .preference(preferenceString);

        // If local=true and no preference is specified then query string contains "_local"
        assertTrue(localOnlyBuilder.toString().contains("_local"));
        // If local=false and a preference is specified then query string contains the preference and not "_local"
        String preferenceOnlyString = preferenceOnlyBuilder.toString();
        assertFalse(preferenceOnlyString.contains("_local"));
        assertTrue(preferenceOnlyString.contains(encodedPreferenceString));
        // If local=true and a preference is specified then query string contains the preference and not "_local"
        String localWithPreferenceString = localWithPreferenceBuilder.toString();
        assertFalse(localWithPreferenceString.contains("_local"));
        assertTrue(localWithPreferenceString.contains(encodedPreferenceString));
    }
}
