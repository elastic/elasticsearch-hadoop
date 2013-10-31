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
package org.elasticsearch.hadoop.util;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.SettingsAware;

public abstract class ObjectUtils {

    @SuppressWarnings("unchecked")
    public static <T> T instantiate(String className, ClassLoader loader) {
        Assert.hasText(className, "No class name given");
        ClassLoader cl = (loader != null ? loader : ObjectUtils.class.getClassLoader());
        Class<?> clz = null;
        try {
            clz = cl.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(String.format("Cannot load class [%s]", className), e);
        }
        try {
            return (T) clz.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Cannot instantiate class [%s]", className), e);
        }
    }

    public static <T> T instantiate(String className, Settings settings) {
        return instantiate(className, null, settings);
    }

    public static <T> T instantiate(String className, ClassLoader loader, Settings settings) {
        T obj = instantiate(className, loader);

        if (obj instanceof SettingsAware) {
            ((SettingsAware) obj).setSettings(settings);
        }

        return obj;
    }

    public static boolean isEmpty(byte[] array) {
        return (array == null || array.length == 0);
    }

    public static boolean isEmpty(String[] array) {
        return (array == null || array.length == 0);
    }
}
