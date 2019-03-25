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

import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.handler.HandlerResult;

/**
 * An error collector that collects responses from code that handles document level bulk failures.
 */
public class BulkWriteErrorCollector implements DelayableErrorCollector<byte[]> {

    private long delayTimeInMillis;

    private boolean isRetry;
    private byte[] currentRetry;
    private String currentMessage;

    public BulkWriteErrorCollector() {
        this.delayTimeInMillis = 0L;
        this.isRetry = false;
        this.currentRetry = null;
        this.currentMessage = null;
    }

    @Override
    public HandlerResult retry() {
        isRetry = true;
        return HandlerResult.HANDLED;
    }

    @Override
    public HandlerResult backoffAndRetry(long timeAmount, TimeUnit timeUnits) {
        long givenDelayTimeInMillis = timeUnits.toMillis(timeAmount);
        if (givenDelayTimeInMillis > delayTimeInMillis) {
            delayTimeInMillis = givenDelayTimeInMillis;
        }
        return retry();
    }

    @Override
    public HandlerResult retry(byte[] retryData) {
        currentRetry = retryData;
        return retry();
    }

    @Override
    public HandlerResult backoffAndRetry(byte[] retryData, long timeAmount, TimeUnit timeUnits) {
        long givenDelayTimeInMillis = timeUnits.toMillis(timeAmount);
        if (givenDelayTimeInMillis > delayTimeInMillis) {
            delayTimeInMillis = givenDelayTimeInMillis;
        }
        return retry(retryData);
    }

    @Override
    public HandlerResult pass(String reason) {
        if (currentMessage == null) {
            currentMessage = reason;
        } else {
            throw new EsHadoopIllegalStateException("Error Handler is attempting to pass with a reason, but a " +
                    "reason already exists! Be sure to return the result of your call to errorCollector.pass(String), " +
                    "and call it only once per call to your Handler!");
        }
        return HandlerResult.PASS;
    }

    public boolean receivedRetries() {
        return isRetry;
    }

    public byte[] getAndClearRetryValue() {
        if (isRetry) {
            byte[] data = null;
            if (currentRetry != null) {
                data = currentRetry;
                currentRetry = null;
            }
            isRetry = false;
            return data;
        } else {
            return null;
        }
    }

    public String getAndClearMessage() {
        String msg = currentMessage;
        currentMessage = null;
        return msg;
    }

    public long getDelayTimeBetweenRetries() {
        return delayTimeInMillis;
    }
}
