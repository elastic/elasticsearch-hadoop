package org.elasticsearch.spark.sql.api.java;

import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.types.util.DataTypeConversions;


abstract class SQLUtils {
	// access given field through Java as Scala doesn't allow it...
	static SchemaRDD baseSchemaRDD(JavaSchemaRDD schemaRDD) {
		return schemaRDD.baseSchemaRDD();
	}
	
	static org.apache.spark.sql.api.java.DataType asJavaDataType(org.apache.spark.sql.catalyst.types.DataType scalaDataType) {
		return DataTypeConversions.asJavaDataType(scalaDataType);
	}
}

