package org.elasticsearch.spark.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.RestRepository;
import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;

abstract class Utils {
	static FieldType extractType(Field field) {
		return field.type();
	}
}

