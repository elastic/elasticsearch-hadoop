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
import java.security.Principal;
import java.util.Collections;
import javax.security.auth.Subject;

/**
 * Operates by retrieving the currently set subject in a way that ES-Hadoop understands, or
 * a blank subject if there isn't currently one available.
 */
public class JdkUserProvider implements UserProvider {

    @Override
    public User getUser() {
        AccessControlContext acc = AccessController.getContext();
        Subject subject = Subject.getSubject(acc);
        if (subject == null) {
            subject = new Subject(false, Collections.<Principal>emptySet(), Collections.emptySet(), Collections.emptySet());
        }
        return new JdkUser(subject);
    }
}
