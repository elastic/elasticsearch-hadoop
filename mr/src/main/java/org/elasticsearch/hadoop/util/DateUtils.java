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
package org.elasticsearch.hadoop.util;

import java.lang.reflect.Method;
import java.util.Calendar;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility used for parsing date ISO8601.
 * Morphed into a runtime bridge over possible ISO8601 (simply because the spec is too large, especially when considering the various optional formats).
 */
public abstract class DateUtils {

    public static boolean printed = false;

    private final static boolean jodaTimeAvailable = ObjectUtils.isClassPresent("org.joda.time.format.ISODateTimeFormat", DateUtils.class.getClassLoader());

    private static abstract class Jdk6 {
        // Parses ISO date through the JDK XML bind class. However the spec doesn't support all ISO8601 formats which this class tries to address
        // in particular Time offsets from UTC are available in 3 forms:
        // The offset from UTC is appended to the time in the same way that 'Z' was above, in the form ±[hh]:[mm], ±[hh][mm], or ±[hh].
        //
        // XML Bind supports only the first one.
        public static Calendar parseDate(String value) {
            // check for colon in the time offset
            int timeZoneIndex = value.indexOf("T");
            if (timeZoneIndex > 0) {
                int sign = value.indexOf("+", timeZoneIndex);
                if (sign < 0) {
                    sign = value.indexOf("-", timeZoneIndex);
                }

                // +4 means it's either hh:mm or hhmm
                if (sign > 0) {
                    // +3 points to either : or m
                    int colonIndex = sign + 3;
                    // +hh - need to add :mm
                    if (colonIndex >= value.length()) {
                        value = value + ":00";
                    }
                    else if (value.charAt(colonIndex) != ':') {
                        value = value.substring(0, colonIndex) + ":" + value.substring(colonIndex);
                    }
                }
            }

            return DatatypeConverter.parseDateTime(value);
        }
    }

    private static abstract class JodaTime {

        private static final Object DATE_OPTIONAL_TIME_FORMATTER;
        private static final Method PARSE_DATE_TIME;
        private static final Method TO_CALENDAR;

        private static final boolean INITIALIZED;

        static {
            boolean init = false;
            Method parseDateTime = null, toCalendar = null;
            Object dotf = null;
            try {
                ClassLoader cl = JodaTime.class.getClassLoader();
                Class<?> FORMAT_CLASS = ObjectUtils.loadClass("org.joda.time.format.ISODateTimeFormat", cl);
                Method DATE_OPTIONAL_TIME = ReflectionUtils.findMethod(FORMAT_CLASS, "dateOptionalTimeParser");
                dotf = ReflectionUtils.invoke(DATE_OPTIONAL_TIME, null);
                parseDateTime = ReflectionUtils.findMethod(dotf.getClass(), "parseDateTime", String.class);
                Class<?> DATE_TIME_CLASS = ObjectUtils.loadClass("org.joda.time.DateTime", cl);
                toCalendar = ReflectionUtils.findMethod(DATE_TIME_CLASS, "toGregorianCalendar");
                init = true;
            } catch (Exception ex) {
                // log exception

            }

            DATE_OPTIONAL_TIME_FORMATTER = dotf;
            PARSE_DATE_TIME = parseDateTime;
            TO_CALENDAR = toCalendar;
            INITIALIZED = init;
        }

        public static Calendar parseDate(String value) {
            Object dt = ReflectionUtils.invoke(PARSE_DATE_TIME, DATE_OPTIONAL_TIME_FORMATTER, value);
            return ReflectionUtils.invoke(TO_CALENDAR, dt);
        }
    }

    public static Calendar parseDate(String value) {
        if (!printed) {
            printed = true;
            Log log = LogFactory.getLog(DateUtils.class);
            if (jodaTimeAvailable && JodaTime.INITIALIZED) {
                log.info("Joda library available in the classpath; using it for date/time handling...");
            }
            else {
                // be silent otherwise
            }
        }

        return (jodaTimeAvailable && JodaTime.INITIALIZED) ? JodaTime.parseDate(value) : Jdk6.parseDate(value);
    }
}