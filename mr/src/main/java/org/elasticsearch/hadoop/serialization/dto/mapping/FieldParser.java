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

package org.elasticsearch.hadoop.serialization.dto.mapping;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.serialization.FieldType;

/**
 * All logic pertaining to parsing Elasticsearch schemas and rendering Mapping and Field objects.
 */
public final class FieldParser {

    private FieldParser() {
        // No instances allowed
    }

    public static MappingSet parseTypedMappings(Map<String, Object> content) {
        return parseMappings(content, true);
    }

    public static MappingSet parseTypelessMappings(Map<String, Object> content) {
        return parseMappings(content, false);
    }

    /**
     * Convert the deserialized mapping request body into an object
     * @param content entire mapping request body for all indices and types
     * @param includeTypeName true if the given content to be parsed includes type names within the structure,
     *                        or false if it is in the typeless format
     * @return MappingSet for that response.
     */
    public static MappingSet parseMappings(Map<String, Object> content, boolean includeTypeName) {
        Iterator<Map.Entry<String, Object>> indices = content.entrySet().iterator();
        List<Mapping> indexMappings = new ArrayList<Mapping>();
        while(indices.hasNext()) {
            // These mappings are ordered by index, then optionally type.
            parseIndexMappings(indices.next(), indexMappings, includeTypeName);
        }
        return new MappingSet(indexMappings);
    }

    private static void parseIndexMappings(Map.Entry<String, Object> indexToMappings, List<Mapping> collector, boolean includeTypeName) {
        // get Index name from key, mappings fields are in value
        String indexName = indexToMappings.getKey();

        // The value should be a singleton map with the key "mappings" mapped to the types/mappings
        // Get the singleton map first
        if (!(indexToMappings.getValue() instanceof Map)) {
            throw new EsHadoopIllegalArgumentException("invalid mapping received " + indexToMappings + "; Invalid mapping structure for [" + indexName + "]");
        }
        Map<String, Object> mappingsObject = (Map<String, Object>) indexToMappings.getValue();

        // Get the types/mappings from the singleton map
        if (!(mappingsObject.get("mappings") instanceof Map)) {
            throw new EsHadoopIllegalArgumentException("invalid mapping received " + indexToMappings + "; Missing mappings under [" + indexName + "]");
        }
        Map<String, Object> mappingEntries = (Map<String, Object>) mappingsObject.get("mappings");

        // Iterate over the mappings to collect their names and contents
        // In versions of ES that have a single type system, there will either
        // be a single named mapping or a single unnamed mapping returned.
        if (includeTypeName) {
            // Every entry within mappingEntries is a type name mapped to the actual mappings
            for (Map.Entry<String, Object> typeToMapping : mappingEntries.entrySet()) {
                String typeName = typeToMapping.getKey();
                Mapping mapping = parseMapping(indexName, typeName, typeToMapping);
                collector.add(mapping);
            }
        } else {
            // Everything under mappingEntries is the contents of a singular actual mapping
            String typeName = MappingSet.TYPELESS_MAPPING_NAME;
            // I can't even describe in english what I'm doing anymore
            if (mappingsObject.entrySet().size() > 1) {
                throw new EsHadoopIllegalArgumentException("invalid mapping received " + indexToMappings + "; Index [" + indexName +
                        "] contains invalid mapping structure.");
            }
            Map.Entry<String, Object> unnamedMapping = mappingsObject.entrySet().iterator().next();
            Mapping mapping = parseMapping(indexName, typeName, unnamedMapping);
            collector.add(mapping);
        }
    }

    private static Mapping parseMapping(String indexName, String typeName, Map.Entry<String, Object> mapping) {
        // Parse the mapping fields
        Field field = parseField(mapping, null);
        if (field == null) {
            throw new EsHadoopIllegalArgumentException("Could not parse mapping contents from [" + mapping + "]");
        }
        return new Mapping(indexName, typeName, field.properties());
    }

    private static Field parseField(Map.Entry<String, Object> entry, String previousKey) {
        // can be "type" or field name
        String key = entry.getKey();
        Object value = entry.getValue();

        // nested object
        if (value instanceof Map) {
            Map<String, Object> content = (Map<String, Object>) value;
            // default field type for a map
            FieldType fieldType = FieldType.OBJECT;

            // see whether the field was declared
            Object type = content.get("type");
            if (type instanceof String) {
                fieldType = FieldType.parse(type.toString());

                if (FieldType.isRelevant(fieldType)) {
                    // primitive types are handled on the spot
                    // while compound ones are not
                    if (!FieldType.isCompound(fieldType)) {
                        return new Field(key, fieldType);
                    }
                }
                else {
                    return null;
                }
            }

            // check if it's a join field since these are special
            if (FieldType.JOIN == fieldType) {
                return new Field(key, fieldType, new Field[]{new Field("name", FieldType.KEYWORD), new Field("parent", FieldType.KEYWORD)});
            }

            // compound type - iterate through types
            List<Field> fields = new ArrayList<Field>(content.size());
            for (Map.Entry<String, Object> e : content.entrySet()) {
                if (e.getValue() instanceof Map) {
                    Field fl = parseField(e, key);
                    if (fl != null && fl.type() == FieldType.OBJECT && "properties".equals(fl.name()) && !isFieldNamedProperties(e.getValue())) {
                        // use the enclosing field (as it might be nested)
                        return new Field(key, fieldType, fl.properties());
                    }
                    if (fl != null) {
                        fields.add(fl);
                    }
                }
            }
            return new Field(key, fieldType, fields);
        }


        throw new EsHadoopIllegalArgumentException("invalid map received " + entry);
    }

    private static boolean isFieldNamedProperties(Object fieldValue){
        if(fieldValue instanceof Map){
            Map<String,Object> fieldValueAsMap = ((Map<String, Object>)fieldValue);
            if((fieldValueAsMap.containsKey("type") && fieldValueAsMap.get("type") instanceof String)
                    || (fieldValueAsMap.containsKey("properties") && !isFieldNamedProperties(fieldValueAsMap.get("properties")))) return true;
        }
        return false;
    }

}
