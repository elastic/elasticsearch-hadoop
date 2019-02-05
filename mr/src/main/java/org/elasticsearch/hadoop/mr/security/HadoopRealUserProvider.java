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

package org.elasticsearch.hadoop.mr.security;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.security.User;
import org.elasticsearch.hadoop.security.UserProvider;

/**
 * When queried for a user object, this provider will obtain the currently logged in user,
 * returning it if it is a regular user, or if it is a PROXY user, will retrieve the lowest
 * level real user from it that has actual credentials.
 */
public class HadoopRealUserProvider extends UserProvider {

    private static Log LOG = LogFactory.getLog(HadoopRealUserProvider.class);

    @Override
    public User getUser() {
        try {
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            if (!UserGroupInformation.AuthenticationMethod.PROXY.equals(currentUser.getAuthenticationMethod())) {
                return new HadoopUser(currentUser, getSettings());
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Current user [" + currentUser.getUserName() + "] is proxy user. Retrieving real user.");
                }
                // Proxy user
                UserGroupInformation realUser = currentUser.getRealUser();
                // Keep getting the real user until we get one that is not a PROXY based auth
                while (realUser != null && UserGroupInformation.AuthenticationMethod.PROXY.equals(realUser.getAuthenticationMethod())) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Found nested proxy user [" + realUser.getUserName() + "]. Checking next real user.");
                    }
                    realUser = realUser.getRealUser();
                }
                // If there is no real user underneath the proxy user, we should treat this like an error case.
                if (realUser == null) {
                    throw new EsHadoopException("Could not locate a real user under the current user [" + currentUser + "].");
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Found real user [" + realUser.getUserName() + "] with auth method of [" +
                                realUser.getAuthenticationMethod() + "]");
                    }
                    return new HadoopUser(realUser, getSettings());
                }
            }
        } catch (IOException e) {
            throw new EsHadoopException("Could not retrieve the current user", e);
        }
    }
}
