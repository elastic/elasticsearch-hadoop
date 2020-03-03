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

package org.elasticsearch.hadoop.security;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.security.UserGroupInformation;
import org.elasticsearch.hadoop.EsHadoopException;

/**
 * Utility methods for dealing with Hadoop UGI in integration tests
 */
public class UgiUtil {

    /**
     * Calls the UserGroupInformation.reset() static method by reflection to clear the static
     * configuration values and login users.
     */
    public static void resetUGI() {
        try {
            Method reset = UserGroupInformation.class.getDeclaredMethod("reset");
            reset.setAccessible(true);
            reset.invoke(null);
        } catch (NoSuchMethodException e) {
            throw new EsHadoopException("Could not reset UGI", e);
        } catch (IllegalAccessException e) {
            throw new EsHadoopException("Could not reset UGI", e);
        } catch (InvocationTargetException e) {
            throw new EsHadoopException("Could not reset UGI", e);
        }
    }

}
