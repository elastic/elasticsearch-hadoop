package org.elasticsearch.hadoop.pig;

import java.util.Calendar;

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
	
	static String convertToES(Object pigDate) {
		return (pig11Available ? Pig11OrHigherConverter.convertToES(pigDate) : PigUpTo10Converter.convertToES(pigDate));
	}
	
	static Object convertFromES(String esDate) {
		return (pig11Available ? Pig11OrHigherConverter.convertFromES(esDate) : PigUpTo10Converter.convertFromES(esDate));
	}
	
	private static abstract class PigUpTo10Converter {
		static String convertToES(Object pigDate) {
			if (pigDate instanceof Number) {
				Calendar cal = Calendar.getInstance();
				cal.setTimeInMillis(((Number) pigDate).longValue());
				return DatatypeConverter.printDateTime(cal);
			}
			if (pigDate instanceof String) {
				return ((String) pigDate);
			}
			throw new IllegalArgumentException(String.format("Cannot convert [%s] to date", pigDate));
		}
		
		static Object convertFromES(String esDate) {
			return DatatypeConverter.parseDateTime(esDate).getTimeInMillis();
		}
	}
	
	private static abstract class Pig11OrHigherConverter {
		static String convertToES(Object pigDate) {
			DateTime dt = (DateTime) pigDate;
			return dt.toString(ISODateTimeFormat.dateOptionalTimeParser());
		}
		
		static Object convertFromES(String esDate) {
			return ISODateTimeFormat.dateOptionalTimeParser().parseDateTime(esDate);
		}
	}
}