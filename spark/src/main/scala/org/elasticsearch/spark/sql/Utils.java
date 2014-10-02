package org.elasticsearch.spark.sql;

import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;

abstract class Utils {
	static FieldType extractType(Field field) {
		return field.type();
	}
}

