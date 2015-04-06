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
package org.elasticsearch.hadoop.yarn.compat;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.elasticsearch.hadoop.yarn.util.ReflectionUtils;
import org.elasticsearch.hadoop.yarn.util.YarnUtils;

/**
 * Series of compatibility utils around YARN classes...
 */
public abstract class YarnCompat {

    // introduced in 2.4 (not 2.2 which might be used in some distros)
    private static String[] DEFAULT_PLATFORM_APPLICATION_CLASSPATH;
    private static String CLASS_PATH_SEPARATOR;

    private static final Method NMCLIENT_SET_TOKEN_CACHE;
    private static final Method AMRMCLIENT_SET_TOKEN_CACHE;
    private static final Method ENVIRONMENT_$$;
    private static final Method SET_VIRTUAL_CORES;
    private static final Method APP_SUBMISSION_CTX;


    static {
        SET_VIRTUAL_CORES = ReflectionUtils.findMethod(Resource.class, "setVirtualCores", int.class);
        NMCLIENT_SET_TOKEN_CACHE = ReflectionUtils.findMethod(NMClient.class, "setNMTokenCache", NMTokenCache.class);
        AMRMCLIENT_SET_TOKEN_CACHE = ReflectionUtils.findMethod(AMRMClient.class, "setNMTokenCache", NMTokenCache.class);
        ENVIRONMENT_$$ = ReflectionUtils.findMethod(Environment.class, "$$", (Class<?>[]) null);
        APP_SUBMISSION_CTX = ReflectionUtils.findMethod(ApplicationSubmissionContext.class, "setApplicationTags", Set.class);
    }

    // set virtual cores is not available on all Hadoop 2.x distros
    public static void setVirtualCores(Configuration cfg, Resource resource, int vCores) {
        if (SET_VIRTUAL_CORES != null) {
            ReflectionUtils.invoke(SET_VIRTUAL_CORES, resource, YarnUtils.minVCores(cfg, vCores));
        }
    }

    public static Resource resource(Configuration cfg, int memory, int cores) {
        Resource resource = Records.newRecord(Resource.class);
        resource.setMemory(YarnUtils.minMemory(cfg, memory));
        setVirtualCores(cfg, resource, cores);
        return resource;
    }

    public static String[] DEFAULT_PLATFORM_APPLICATION_CLASSPATH() {
        if (DEFAULT_PLATFORM_APPLICATION_CLASSPATH == null) {
            Field field = ReflectionUtils.findField(YarnConfiguration.class, "DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH");
            if (field != null) {
                ReflectionUtils.makeAccessible(field);
                DEFAULT_PLATFORM_APPLICATION_CLASSPATH = ReflectionUtils.getField(field, null);
            }
            else {
                DEFAULT_PLATFORM_APPLICATION_CLASSPATH = YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH;
            }
        }

        return DEFAULT_PLATFORM_APPLICATION_CLASSPATH;
    }

    public static String CLASS_PATH_SEPARATOR() {
        if (CLASS_PATH_SEPARATOR == null) {
            Field field = ReflectionUtils.findField(ApplicationConstants.class, "CLASS_PATH_SEPARATOR");
            if (field != null) {
                ReflectionUtils.makeAccessible(field);
                CLASS_PATH_SEPARATOR = ReflectionUtils.getField(field, null);
            }
            else {
                // always use the *Nix separator since the chances the server runs on it are significantly higher than on Windows
                CLASS_PATH_SEPARATOR = ":";
            }
        }

        return CLASS_PATH_SEPARATOR;
    }

    public static void setNMTokenCache(AMRMClient<?> client, NMTokenCache tokenCache) {
        if (AMRMCLIENT_SET_TOKEN_CACHE != null) {
            ReflectionUtils.invoke(AMRMCLIENT_SET_TOKEN_CACHE, client, tokenCache);
        }
    }

    public static void setNMTokenCache(NMClient client, NMTokenCache tokenCache) {
        if (NMCLIENT_SET_TOKEN_CACHE != null) {
            ReflectionUtils.invoke(NMCLIENT_SET_TOKEN_CACHE, client, tokenCache);
        }
    }

    public static String $$(Environment env) {
        if (ENVIRONMENT_$$ != null) {
            return ReflectionUtils.invoke(ENVIRONMENT_$$, env, (Object[]) null);
        }
        else {
            // prefer *nix separator
            return "$" + env.name();
        }
    }

    public static void setApplicationTags(ApplicationSubmissionContext appContext, Set<String> appTags) {
        if (APP_SUBMISSION_CTX != null) {
            ReflectionUtils.invoke(APP_SUBMISSION_CTX, appContext, appTags);
        }
    }
}