package org.elasticsearch.hadoop.serialization.dto.mapping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.serialization.FieldType;

/**
 * Object representation of all mappings available under a given set of indices. Manages storing each mapping
 * by its index and type for fast lookups as well as resolving multiple mappings down into one combined schema.
 */
public class MappingSet implements Serializable {

    private static final String RESOLVED_MAPPING_NAME = "*";

    private final boolean empty;
    private final Map<String, Map<String, Mapping>> indexTypeMap = new HashMap<String, Map<String, Mapping>>();
    private final Mapping resolvedSchema;

    public MappingSet(List<Field> fields) {
        if (fields.isEmpty()) {
            this.empty = true;
            this.resolvedSchema = new Mapping(RESOLVED_MAPPING_NAME, Field.NO_FIELDS);
        } else {
            this.empty = false;
            for (Field field : fields) {
                String indexName = field.name();
                Field[] mappings = field.properties();
                Map<String, Mapping> mappingsToSchema = new HashMap<String, Mapping>();
                this.indexTypeMap.put(indexName, mappingsToSchema);

                for (Field mappingHeader : mappings) {
                    // There's only one mapping Header named "mappings". Unwrap it to get the actual mappings.
                    for (Field mapping : mappingHeader.properties()) {
                        mappingsToSchema.put(mapping.name(), new Mapping(mapping.name(), mapping.properties()));
                    }
                }

            }
            this.resolvedSchema = mergeMappings(fields);
        }
    }

    private static Mapping mergeMappings(List<Field> fields) {
        Map<String, Object[]> fieldMap = new LinkedHashMap<String, Object[]>();
        for (Field rootField : fields) {
            Field[] props = rootField.properties();
            // handle the common case of mapping by removing the first field (mapping.)
            if (props.length > 0 && props[0] != null && "mappings".equals(props[0].name()) && FieldType.OBJECT.equals(props[0].type())) {
                // can't return the type as it is an object of properties
                Field[] mappings = props[0].properties();

                for (Field mapping : mappings) {
                    // At this point we have the root mapping info
                    for (Field field : mapping.properties()) {
                        addToFieldTable(field, "", fieldMap);
                    }
                }
            }
        }
        Field[] collapsed = collapseFields(fieldMap);
        return new Mapping(RESOLVED_MAPPING_NAME, collapsed);
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
                throw new EsHadoopIllegalArgumentException("Incompatible types found in multi-mapping: " +
                        "Field ["+fullName+"] has conflicting types of ["+previousField.type()+"] and ["+
                        field.type()+"].");
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
