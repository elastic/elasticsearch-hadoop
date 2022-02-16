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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Lists;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.serialization.JsonUtils;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.util.BytesArray;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class ScrollQueryTest {

    public void test(boolean firstScrollReturnsHits) throws Exception {
        RestRepository repository = mockRepository(firstScrollReturnsHits);
        ScrollReader scrollReader = Mockito.mock(ScrollReader.class);

        String query = "/index/type/_search?scroll=10m&etc=etc";
        BytesArray body = new BytesArray("{}");
        long size = 100;

        ScrollQuery scrollQuery = new ScrollQuery(repository, query, body, size, scrollReader);

        Assert.assertTrue(scrollQuery.hasNext());
        Assert.assertEquals("value", JsonUtils.query("field").apply(scrollQuery.next()[1]));
        Assert.assertFalse(scrollQuery.hasNext());
        scrollQuery.close();
        Mockito.verify(repository).close();
        Stats stats = scrollQuery.stats();
        Assert.assertEquals(1, stats.docsReceived);
        Assert.assertEquals(1, scrollQuery.getRead());
    }

    @Test
    public void testWithEmptyFirstScroll() throws Exception {
        test(false);
    }

    @Test
    public void testWithNonEmptyFirstScroll() throws Exception {
        test(true);
    }

    private RestRepository mockRepository(boolean firstScrollReturnsHits) throws Exception {
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("field", "value");
        String id = "1";
        Object[] hit = new Object[]{id, data};

        RestRepository mocked = Mockito.mock(RestRepository.class);

        ScrollReader.Scroll start = new ScrollReader.Scroll("abcd", 10,
                firstScrollReturnsHits ? Collections.singletonList(hit) : Collections.<Object[]>emptyList(),
                5, 5);

        Mockito.doReturn(start).when(mocked).scroll(Matchers.anyString(), Matchers.any(BytesArray.class), Matchers.any(ScrollReader.class));

        ScrollReader.Scroll middle = new ScrollReader.Scroll("efgh", 10, Collections.<Object[]>emptyList(), 3, 3);
        Mockito.doReturn(middle).when(mocked).scroll(Matchers.eq("abcd"), Matchers.any(ScrollReader.class));
        ScrollReader.Scroll end = new ScrollReader.Scroll("ijkl", 10,
                firstScrollReturnsHits ? Collections.<Object[]>emptyList() : Collections.singletonList(hit),
                2, 1);
        Mockito.doReturn(end).when(mocked).scroll(Matchers.eq("efgh"), Matchers.any(ScrollReader.class));
        ScrollReader.Scroll finalScroll = new ScrollReader.Scroll("mnop", 10, true);
        Mockito.doReturn(finalScroll).when(mocked).scroll(Matchers.eq("ijkl"), Matchers.any(ScrollReader.class));

        RestClient mockClient = Mockito.mock(RestClient.class);
        Mockito.when(mockClient.deleteScroll(Matchers.eq("mnop"))).thenReturn(true);
        Mockito.when(mockClient.deleteScroll(Matchers.anyString())).thenReturn(false);

        Mockito.doReturn(mockClient).when(mocked).getRestClient();



        return mocked;
    }

    @Test
    public void testFrozen() throws Exception {
        // Frozen indices return a null scroll
        RestRepository repository = mockRepositoryFrozenIndex();
        ScrollReader scrollReader = Mockito.mock(ScrollReader.class);

        String query = "/index/type/_search?scroll=10m&etc=etc";
        BytesArray body = new BytesArray("{}");
        long size = 100;

        ScrollQuery scrollQuery = new ScrollQuery(repository, query, body, size, scrollReader);

        Assert.assertFalse(scrollQuery.hasNext());
        scrollQuery.close();
        Mockito.verify(repository).close();
        Stats stats = scrollQuery.stats();
        Assert.assertEquals(0, stats.docsReceived);
    }

    private RestRepository mockRepositoryFrozenIndex() throws Exception {
        RestRepository mocked = Mockito.mock(RestRepository.class);
        Mockito.doReturn(null).when(mocked).scroll(Matchers.anyString(), Matchers.any(BytesArray.class), Matchers.any(ScrollReader.class));
        RestClient mockClient = Mockito.mock(RestClient.class);
        Mockito.when(mockClient.deleteScroll(Matchers.eq("mnop"))).thenReturn(true);
        Mockito.when(mockClient.deleteScroll(Matchers.anyString())).thenReturn(false);
        Mockito.doReturn(mockClient).when(mocked).getRestClient();
        return mocked;
    }
}