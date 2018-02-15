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
package org.elasticsearch.hadoop.hive.pushdown;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.hive.EsStoragePredicateHandler;
import org.elasticsearch.hadoop.util.unit.Booleans;

public class Utils {

    private static final Log log = LogFactory.getLog(Utils.class);

    /**
     * is pushdown optimization, default true
     */
    static String DATA_SOURCE_PUSH_DOWN = "es.internal.hive.pushdown";

    /**
     * pushdown class path，default org.elasticsearch.hadoop.hive.EsStoragePredicateHandler
     */
    static String DATA_SOURCE_PUSH_DOWN_CLASS = "es.internal.hive.pushdown.class";

    /**
     * sargable parser class path, default org.elasticsearch.hadoop.hive.pushdown.SargableParser
     */
    static String DATA_SOURCE_SARGABLE_CLASS_PATH = "es.internal.hive.sargable.class";

    public static boolean isPushDown(Settings cfg) {
        boolean isPushdown = Booleans.parseBoolean(cfg.getProperty(DATA_SOURCE_PUSH_DOWN), true);
        log.info("[Pushdown] " + DATA_SOURCE_PUSH_DOWN + ":" + isPushdown);
        return isPushdown;
    }

    public static SargableParser getSargableParser(Settings cfg) {
        try {
            String sargbleClass = cfg.getProperty(DATA_SOURCE_SARGABLE_CLASS_PATH);

            if (sargbleClass == null) {
                sargbleClass = SargableParser.class.getCanonicalName();
            }
            Class<?> aClass = Class.forName(sargbleClass);

            log.info("[Pushdown][CLASSPATH] " + DATA_SOURCE_SARGABLE_CLASS_PATH + ":" + sargbleClass);
            return (SargableParser) aClass.newInstance();
        } catch (Exception e) {
            log.info("[Pushdown] " + e.getMessage(), e);
        }
        return null;
    }

    public static PredicateHandler getPredicateHander(Settings cfg) {
        try {
            String pushdownClass = cfg.getProperty(DATA_SOURCE_PUSH_DOWN_CLASS);

            if (pushdownClass == null) {
                pushdownClass = EsStoragePredicateHandler.class.getCanonicalName();
            }
            Class<?> aClass = Class.forName(pushdownClass);
            log.info("[Pushdown][CLASSPATH] " + DATA_SOURCE_PUSH_DOWN_CLASS + ":" + pushdownClass);
            return (PredicateHandler) aClass.newInstance();
        } catch (Exception e) {
            log.info("[Pushdown] " + e.getMessage(), e);
        }
        return null;
    }

}
