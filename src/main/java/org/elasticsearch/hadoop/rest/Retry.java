/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.rest;

import org.apache.commons.httpclient.HttpStatus;

public class Retry {

    private final long retryTime;
    private final int retryLimit;
    private int retryCount = 0;

    public Retry(long retryTime, int retryLimit) {
        this.retryTime = retryTime;
        this.retryLimit = retryLimit;
    }

    public boolean retry(int httpStatus) {
        // everything fine, no need to retry
        if (httpStatus < HttpStatus.SC_MULTI_STATUS) {
            return false;
        }

        switch (httpStatus) {
        // ES is busy, allow retries
        case HttpStatus.SC_SERVICE_UNAVAILABLE:
            if (++retryCount < retryLimit) {
                try {
                    Thread.sleep(retryTime);
                    return true;
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        default:
            return false;
        }
    }
}
