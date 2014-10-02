package org.elasticsearch.spark.sql.api.java;

import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

abstract class SQLUtils {
	// access given field through Java as Scala doesn't allow it...
	static SchemaRDD baseSchemaRDD(JavaSchemaRDD schemaRDD) {
		return schemaRDD.baseSchemaRDD();
	}
}

