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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.serialization.FieldType;

/**
 * Object representation of all mappings available under a given set of indices. Manages storing each mapping
 * by its index and type for fast lookups as well as resolving multiple mappings down into one combined schema.
 */
public class MappingSet implements Serializable {

    private static final String RESOLVED_INDEX_NAME = "*";
    private static final String RESOLVED_MAPPING_NAME = "*";

    // For versions of Elasticsearch that do not have types.
    public static final String TYPELESS_MAPPING_NAME = "";

    private final boolean empty;
    private final Map<String, Map<String, Mapping>> indexTypeMap = new HashMap<String, Map<String, Mapping>>();
    private final Mapping resolvedSchema;

    public MappingSet(List<Mapping> mappings) {
        if (mappings.isEmpty()) {
            this.empty = true;
            this.resolvedSchema = new Mapping(RESOLVED_INDEX_NAME, RESOLVED_MAPPING_NAME, Field.NO_FIELDS);
        } else {
            this.empty = false;
            for (Mapping mapping: mappings) {
                String indexName = mapping.getIndex();
                String typeName = mapping.getType();

                // Create a new mapping of type name to schema and register it with the index type map.
                Map<String, Mapping> mappingsToSchema = this.indexTypeMap.get(indexName);
                if (mappingsToSchema == null) {
                    mappingsToSchema = new HashMap<String, Mapping>();
                    this.indexTypeMap.put(indexName, mappingsToSchema);
                }

                // Make sure that we haven't encountered this type already
                if (mappingsToSchema.containsKey(typeName)) {
                    String message;
                    if (typeName.equals(TYPELESS_MAPPING_NAME)) {
                        message = String.format("Invalid mapping set given. Multiple unnamed mappings in the index [%s].",
                                indexName);
                    } else {
                        message = String.format("Invalid mapping set given. Multiple mappings of the same name [%s] in the index [%s].",
                                typeName, indexName);
                    }
                    throw new EsHadoopIllegalArgumentException(message);
                }

                mappingsToSchema.put(typeName, mapping);
            }
            this.resolvedSchema = mergeMappings(mappings);
        }
    }

    private static Mapping mergeMappings(List<Mapping> mappings) {
        Map<String, Object[]> fieldMap = new LinkedHashMap<String, Object[]>();
        for (Mapping mapping: mappings) {
            for (Field field : mapping.getFields()) {
                addToFieldTable(field, "", fieldMap);
            }
        }
        Field[] collapsed = collapseFields(fieldMap);
        return new Mapping(RESOLVED_INDEX_NAME, RESOLVED_MAPPING_NAME, collapsed);
    }

    @SuppressWarnings("unchecked")
    private static void addToFieldTable(Field field, String parent, Map<String, Object[]> fieldTable) {
        String fullName = parent + field.name();
        Object[] entry = fieldTable.get(fullName);
        if (entry == null) {
            // Haven't seen field yet.
            if (FieldType.isCompound(field.type())) {
                // visit its children
                Map<String, Object[]> subTable =  new LinkedHashMap<String, Object[]>();
                entry = new Object[]{field, subTable};
                String prefix = fullName + ".";
                for (Field subField : field.properties()) {
                    addToFieldTable(subField, prefix, subTable);
                }
            } else {
                // note that we saw it
                entry = new Object[]{field};
            }
            fieldTable.put(fullName, entry);
        } else {
            // We've seen this field before.
            Field previousField = (Field)entry[0];
            // ensure that it doesn't conflict
            if (!previousField.type().equals(field.type())) {
                // Attempt to resolve field type conflicts by upcasting fields to a common "super type"
                FieldType resolvedType = resolveTypeConflict(fullName, previousField.type(), field.type());
                // If successful, update the previous field entry with the updated field type
                if (!previousField.type().equals(resolvedType)) {
                    previousField = new Field(previousField.name(), resolvedType, previousField.properties());
                    entry[0] = previousField;
                }
            }
            // If it does not conflict, visit it's children if it has them
            if (FieldType.isCompound(field.type())) {
                Map<String, Object[]> subTable = (Map<String, Object[]>)entry[1];
                String prefix = fullName + ".";
                for (Field subField : field.properties()) {
                    addToFieldTable(subField, prefix, subTable);
                }
            }
        }
    }

    private static FieldType resolveTypeConflict(String fullName, FieldType existing, FieldType incoming) {
        // Prefer to upcast the incoming field to the existing first
        LinkedHashSet<FieldType> incomingSuperTypes = incoming.getCastingTypes();
        if (incomingSuperTypes.contains(existing)) {
            // Incoming can be cast to existing.
            return existing;
        }
        // See if existing can be upcast to the incoming field's type next
        LinkedHashSet<FieldType> existingSuperTypes = existing.getCastingTypes();
        if (existingSuperTypes.contains(incoming)) {
            // Existing can be cast to incoming
            return incoming;
        }
        // Finally, Try to pick the lowest common super type for both fields if it exists
        if (incomingSuperTypes.size() > 0 && existingSuperTypes.size() > 0) {
            LinkedHashSet<FieldType> combined = new LinkedHashSet<FieldType>(incomingSuperTypes);
            combined.retainAll(existingSuperTypes);
            if (combined.size() > 0) {
                return combined.iterator().next();
            }
        }
        // If none of the above options succeed, the fields are conflicting
        throw new EsHadoopIllegalArgumentException("Incompatible types found in multi-mapping: " +
                "Field ["+fullName+"] has conflicting types of ["+existing+"] and ["+
                incoming+"].");
    }

    @SuppressWarnings("unchecked")
    private static Field[] collapseFields(Map<String, Object[]> fieldTable) {
        List<Field> fields = new ArrayList<Field>();
        for (Map.Entry<String, Object[]> fieldInfo : fieldTable.entrySet()) {
            Field currentField = (Field)(fieldInfo.getValue()[0]);
            if (FieldType.isCompound(currentField.type())) {
                Map<String, Object[]> subTable = (Map<String, Object[]>)(fieldInfo.getValue()[1]);
                Field[] children = collapseFields(subTable);
                fields.add(new Field(currentField.name(), currentField.type(), children));
            } else {
                fields.add(currentField);
            }
        }
        return fields.size() == 0 ? Field.NO_FIELDS : fields.toArray(new Field[fields.size()]);
    }

    public Mapping getMapping(String index, String type) {
        Mapping mapping = null;
        Map<String, Mapping> mappings = indexTypeMap.get(index);
        if (mappings != null) {
            mapping = mappings.get(type);
        }
        return mapping;
    }

    /**
     * True if there are no mappings in this mapping set
     */
    public boolean isEmpty() {
        return empty;
    }

    /**
     * Returns the combined schema of all mappings contained within this mapping set.
     */
    public Mapping getResolvedView() {
        return resolvedSchema;
    }

    @Override
    public String toString() {
        return "MappingSet{" +
                "indexTypeMap=" + indexTypeMap +
                ", resolvedSchema=" + resolvedSchema +
                '}';
    }
}
