package org.elasticsearch.spark.sql;

import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;
import org.apache.spark.sql.types.util.DataTypeConversions;

abstract class Utils {
	
	static FieldType extractType(Field field) {
		return field.type();
	}

	static org.apache.spark.sql.catalyst.types.DataType asScalaDataType(org.apache.spark.sql.api.java.DataType javaDataType) {
		return DataTypeConversions.asScalaDataType(javaDataType);
	}

	static org.apache.spark.sql.api.java.DataType asJavaDataType(org.apache.spark.sql.catalyst.types.DataType scalaDataType) {
		return DataTypeConversions.asJavaDataType(scalaDataType);
	}
}

