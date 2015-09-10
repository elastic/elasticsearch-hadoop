package org.elasticsearch.spark.sql;

import org.apache.spark.sql.types.util.DataTypeConversions;
import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;

abstract class Utils {

    static final String ROW_ORDER_PROPERTY = "es.internal.spark.sql.row.order";
    static final String ROOT_LEVEL_NAME = "_";

    static FieldType extractType(Field field) {
        return field.type();
    }

    static org.apache.spark.sql.catalyst.types.DataType asScalaDataType(org.apache.spark.sql.api.java.DataType javaDataType) {
        return DataTypeConversions.asScalaDataType(javaDataType);
    }

    static String camelCaseToDotNotation(String string) {
        StringBuilder sb = new StringBuilder();

        char last = 0;
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            if (Character.isUpperCase(c) && Character.isLowerCase(last)) {
                sb.append(".");
            }
            last = c;
            sb.append(Character.toLowerCase(c));
        }

        return sb.toString();
    }
}

