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

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.elasticsearch.hadoop.EsHadoopException;

/**
 * Provides a platform independent way of accessing user information.
 */
public interface User {

    /**
     * Execute the given action as the user
     * @param action To execute
     * @param <T> The return type from the action
     * @return Whatever was returned from the action
     */
    <T> T doAs(PrivilegedAction<T> action);

    /**
     * Execute the given action as the user
     * @param action To execute
     * @param <T> The return type from the action
     * @return Whatever was returned from the action
     * @throws EsHadoopException Should an exception be thrown during the operation
     */
    <T> T doAs(PrivilegedExceptionAction<T> action) throws EsHadoopException;

    /**
     * @param clusterName The cluster name
     * @return a previously added Elasticsearch authentication token, or null if it does not exist
     */
    EsToken getEsToken(String clusterName);

    /**
     * Adds the given esToken to the user
     * @param esToken Authentication token for Elasticsearch
     */
    void addEsToken(EsToken esToken);

    /**
     * @return the KerberosPrincipal attached to the user, or null if it does not exist
     */
    KerberosPrincipal getKerberosPrincipal();
}