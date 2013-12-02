package org.elasticsearch.hadoop.pig;

import java.util.List;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;
import org.elasticsearch.hadoop.serialization.JdkValueReader;

public class PigValueReader extends JdkValueReader {

	@Override
	public Object addToArray(Object array, List<Object> value) {
		return TupleFactory.getInstance().newTupleNoCopy(value);
	}

	@Override
	protected Object binaryValue(byte[] value) {
		return new DataByteArray(value);
	}

	@Override
	protected Object date(String value) {
		return PigUtils.convertDateFromES(value);
	}
}