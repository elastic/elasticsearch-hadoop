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
package org.elasticsearch.integration.storm;

import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.hadoop.util.unit.TimeValue;

public class Counter {

    private final AtomicInteger counter = new AtomicInteger(0);

    public Counter(int value) {
        counter.set(value);
    }

    public void increment() {
        counter.incrementAndGet();
    }

    public void decrement() {
        counter.decrementAndGet();
        synchronized (counter) {
            counter.notifyAll();
        }
    }

    public boolean decrementAndWait(TimeValue tv) {
        decrement();
        return waitForZero(tv);
    }

    public boolean waitForZero(TimeValue tv) {
        return waitFor(0, tv);
    }

    public boolean is(int value) {
        return counter.get() == value;
    }

    public boolean isZero() {
        return is(0);
    }

    public boolean waitFor(int value, TimeValue tv) {
        boolean timedout = false;
        long waitTime = tv.millis();
        long remainingTime = waitTime;
        long startTime = System.currentTimeMillis();

        while (!timedout && counter.intValue() > value) {
            try {
                synchronized (counter) {
                    counter.wait(remainingTime);
                }
                remainingTime = waitTime - (System.currentTimeMillis() - startTime);
                timedout = remainingTime <= 0;
            } catch (InterruptedException ex) {
                timedout = true;
            }
        }

        return timedout;
    }

    public int get() {
        return counter.get();
    }
}
