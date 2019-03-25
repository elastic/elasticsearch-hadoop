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

package org.elasticsearch.hadoop.rest.bulk.handler;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.hadoop.handler.HandlerResult;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class BulkWriteErrorCollectorTest {

    BulkWriteErrorCollector collector;

    @Before
    public void setUp() throws Exception {
        collector = new BulkWriteErrorCollector();
    }

    @Test
    public void retry() throws Exception {
        assertEquals(HandlerResult.HANDLED, collector.retry());
        assertEquals(true, collector.receivedRetries());
        assertEquals(null, collector.getAndClearMessage());
        assertEquals(null, collector.getAndClearRetryValue());
        assertEquals(0L, collector.getDelayTimeBetweenRetries());
        assertEquals(false, collector.receivedRetries());
    }

    @Test
    public void backoffAndRetry() throws Exception {
        assertEquals(HandlerResult.HANDLED, collector.backoffAndRetry(100L, TimeUnit.MILLISECONDS));
        assertEquals(true, collector.receivedRetries());
        assertEquals(null, collector.getAndClearMessage());
        assertEquals(null, collector.getAndClearRetryValue());
        assertEquals(100L, collector.getDelayTimeBetweenRetries());
        assertEquals(false, collector.receivedRetries());
    }

    @Test
    public void retry1() throws Exception {
        assertEquals(HandlerResult.HANDLED, collector.retry(new byte[]{0, 1, 2, 3, 4, 5}));
        assertEquals(true, collector.receivedRetries());
        assertEquals(null, collector.getAndClearMessage());
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5}, collector.getAndClearRetryValue());
        assertEquals(0L, collector.getDelayTimeBetweenRetries());
        assertEquals(false, collector.receivedRetries());
    }

    @Test
    public void backoffAndRetry1() throws Exception {
        assertEquals(HandlerResult.HANDLED, collector.backoffAndRetry(new byte[]{0, 1, 2, 3, 4, 5}, 100L, TimeUnit.MILLISECONDS));
        assertEquals(true, collector.receivedRetries());
        assertEquals(null, collector.getAndClearMessage());
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5}, collector.getAndClearRetryValue());
        assertEquals(100L, collector.getDelayTimeBetweenRetries());
        assertEquals(false, collector.receivedRetries());
    }

    @Test
    public void pass() throws Exception {
        assertEquals(HandlerResult.PASS, collector.pass("Pass reason"));
        assertEquals(false, collector.receivedRetries());
        assertEquals("Pass reason", collector.getAndClearMessage());
        assertEquals(null, collector.getAndClearRetryValue());
        assertEquals(0L, collector.getDelayTimeBetweenRetries());
    }

}