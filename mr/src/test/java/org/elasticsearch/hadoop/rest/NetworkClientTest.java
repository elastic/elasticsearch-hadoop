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

import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NetworkClientTest {

    @Test
    public void testExecuteRetry() throws Exception {
        TransportFactory mockFactory = mock(TransportFactory.class);
        Transport mockTransport = mock(Transport.class);
        when(mockFactory.create(any(), any(), any())).thenReturn(mockTransport);

        when(mockTransport.execute(any())).thenThrow(new RuntimeException("whoops"));
        NetworkClient networkClient = new NetworkClient(new TestSettings(), mockFactory);
        SimpleRequest simpleRequest = new SimpleRequest(Request.Method.GET, "", "");
        try {
            networkClient.execute(simpleRequest);
            fail("exception should have been thrown");
        } catch (Exception e) {
            // EsHadoopNoNodesLeftException indicates we tried to retry (even if there is just a single node)
            assertEquals(EsHadoopNoNodesLeftException.class, e.getClass());
        }
    }

    @Test
    public void testExecuteNoRetry() throws Exception {
        TransportFactory mockFactory = mock(TransportFactory.class);
        Transport mockTransport = mock(Transport.class);
        when(mockFactory.create(any(), any(), any())).thenReturn(mockTransport);

        when(mockTransport.execute(any())).thenThrow(new RuntimeException("whoops"));
        NetworkClient networkClient = new NetworkClient(new TestSettings(), mockFactory);
        SimpleRequest simpleRequest = new SimpleRequest(Request.Method.GET, "", "");
        try {
            networkClient.execute(simpleRequest, false);
            fail("exception should have been thrown");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Retrying has been disabled. Aborting..."));
            assertEquals(EsHadoopException.class, e.getClass());
        }
    }

}
