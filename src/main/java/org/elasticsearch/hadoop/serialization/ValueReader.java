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
package org.elasticsearch.hadoop.serialization;

import java.util.List;


/**
 * Translates a JSON field to an actual object. Implementations should only handle the conversion and not influence the parser outside the conversion.
 */
public interface ValueReader {

    Object readValue(Parser parser, String value, FieldType esType);

    Object createMap();

    void addToMap(Object map, Object key, Object value);

    Object createArray(FieldType type);

    Object addToArray(Object array, List<Object> values);
}
