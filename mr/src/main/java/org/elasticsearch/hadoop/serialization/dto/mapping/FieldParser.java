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

    /**
     * Convert the deserialized mapping request body into an object
     * @param content entire mapping request body for all indices and types
     * @return MappingSet for that response.
     */
    public static MappingSet parseMapping(Map<String, Object> content) {
        Iterator<Map.Entry<String, Object>> iterator = content.entrySet().iterator();
        List<Field> fields = new ArrayList<Field>();
        while(iterator.hasNext()) {
            Field field = parseField(iterator.next(), null);
            fields.add(field);
        }
        return new MappingSet(fields);
    }

    private static Field skipHeaders(Field field) {
        Field[] props = field.properties();

        // handle the common case of mapping by removing the first field (mapping.)
        if (props.length > 0 && props[0] != null && "mappings".equals(props[0].name()) && FieldType.OBJECT.equals(props[0].type())) {
            // can't return the type as it is an object of properties
            return props[0].properties()[0];
        }
        return field;
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
