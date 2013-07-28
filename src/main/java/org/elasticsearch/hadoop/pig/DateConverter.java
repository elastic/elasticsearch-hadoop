package org.elasticsearch.hadoop.pig;

import javax.xml.bind.DatatypeConverter;

import org.apache.pig.data.DataType;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;


class DateConverter {

	private static boolean pig11Available;
	
	static {
		// initialize
		pig11Available = "datetime".equals(DataType.findTypeName((byte) 30));
	}
	
	static long convertToES(Object pigDate) {
		return (pig11Available ? Pig11OrHigherConverter.convertToES(pigDate) : PigUpTo10Converter.convertToES(pigDate));
	}
	
	static Object convertFromES(String esDate) {
		return (pig11Available ? Pig11OrHigherConverter.convertFromES(esDate) : PigUpTo10Converter.convertFromES(esDate));
	}
	
	private static abstract class PigUpTo10Converter {
		static long convertToES(Object pigDate) {
			if (pigDate instanceof Number) {
				return ((Number) pigDate).longValue();
			}
			throw new IllegalArgumentException(String.format("Cannot convert [%s] to date", pigDate));
		}
		
		static Object convertFromES(String esDate) {
			return DatatypeConverter.parseDateTime(esDate).getTimeInMillis();
		}
	}
	
	private static abstract class Pig11OrHigherConverter {
		static long convertToES(Object pigDate) {
			DateTime dt = (DateTime) pigDate;
			return dt.getMillis();
		}
		
		static Object convertFromES(String esDate) {
			return ISODateTimeFormat.dateTime().parseDateTime(esDate);
		}
	}
}