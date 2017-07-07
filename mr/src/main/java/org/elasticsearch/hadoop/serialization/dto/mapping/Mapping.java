package org.elasticsearch.hadoop.serialization.dto.mapping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.field.FieldFilter;

/**
 * A mapping has a name and a collection of fields.
 */
public class Mapping implements Serializable {

    private final String name;
    private final Field[] fields;

    public Mapping(String name, Collection<Field> fields) {
        this(name, (fields != null ? fields.toArray(new Field[fields.size()]) : Field.NO_FIELDS));
    }

    Mapping(String name, Field[] fields) {
        this.name = name;
        this.fields = fields;
    }

    public String getName() {
        return name;
    }

    public Field[] getFields() {
        return fields;
    }

    /**
     * Filters out fields based on the provided include and exclude information and returns a Mapping object
     * @param includes Field names to explicitly include in the mapping
     * @param excludes Field names to explicitly exclude in the mapping
     * @return this if no fields were filtered, or a new Mapping with the modified field lists.
     */
    public Mapping filter(Collection<String> includes, Collection<String> excludes) {
        if (includes.isEmpty() && excludes.isEmpty()) {
            return this;
        }

        List<Field> filtered = new ArrayList<Field>();
        List<FieldFilter.NumberedInclude> convertedIncludes = FieldFilter.toNumberedFilter(includes);

        boolean intact = true;
        for (Field fl : this.getFields()) {
            intact &= filterField(fl, null, filtered, convertedIncludes, excludes);
        }

        return (intact ? this : new Mapping(this.getName(), filtered));
    }

    private static boolean filterField(Field field, String parentName, List<Field> filtered, Collection<FieldFilter.NumberedInclude> includes, Collection<String> excludes) {
        String fieldName = (parentName != null ? parentName + "." + field.name() : field.name());

        boolean intact = true;

        if (FieldFilter.filter(fieldName, includes, excludes).matched) {
            if (FieldType.isCompound(field.type())) {
                List<Field> nested = new ArrayList<Field>();
                for (Field nestedField : field.properties()) {
                    intact &= filterField(nestedField, field.name(), nested, includes, excludes);
                }
                filtered.add(new Field(field.name(), field.type(), nested));
            }
            else {
                filtered.add(field);
            }
        }
        else {
            intact = false;
        }
        return intact;
    }

    /**
     * Takes a mapping tree and returns a map of all of its fields flattened, and paired with their
     * field types.
     */
    public Map<String, FieldType> flatten() {
        if (fields == null || fields.length == 0) {
            return Collections.<String, FieldType> emptyMap();
        }

        Map<String, FieldType> map = new LinkedHashMap<String, FieldType>();

        for (Field nestedField : fields) {
            addSubFieldToMap(map, nestedField, null);
        }

        return map;
    }

    private static void addSubFieldToMap(Map<String, FieldType> fields, Field field, String parentName) {
        String fieldName = (parentName != null ? parentName + "." + field.name() : field.name());

        fields.put(fieldName, field.type());

        if (FieldType.isCompound(field.type())) {
            for (Field nestedField : field.properties()) {
                addSubFieldToMap(fields, nestedField, fieldName);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("%s=%s", name, Arrays.toString(fields));
    }
}
