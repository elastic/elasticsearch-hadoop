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

import java.security.AccessControlContext;
import java.security.AccessController;
import javax.security.auth.Subject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Operates by retrieving the currently set subject in a way that ES-Hadoop understands, or
 * a blank subject if there isn't currently one available.
 */
public class JdkUserProvider extends UserProvider {

    private static final Log LOG = LogFactory.getLog(JdkUserProvider.class);

    @Override
    public User getUser() {
        AccessControlContext acc = AccessController.getContext();
        Subject subject = Subject.getSubject(acc);
        if (subject == null) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Could not locate existing Subject - Creating new one");
            }
            subject = new Subject();
        } else {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Located existing Subject - " + subject);
            }
        }
        return new JdkUser(subject, getSettings());
    }
}
