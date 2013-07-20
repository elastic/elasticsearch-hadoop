package org.elasticsearch.hadoop.mr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.serialization.Parser;
import org.elasticsearch.hadoop.serialization.SimpleValueReader;
import org.elasticsearch.hadoop.serialization.Parser.Token;

public class WritableValueReader extends SimpleValueReader {

	protected Object list(Parser parser) {
		Token t = parser.currentToken();

		if (t == null) {
			t = parser.nextToken();
		}
		if (t == Token.START_ARRAY) {
			t = parser.nextToken();
		}

		List<Writable> lw = new ArrayList<Writable>();
		for (; t != Token.END_ARRAY; t = parser.nextToken()) {
			lw.add((Writable) read(t, parser));
		}
		
		Writable[] values = (lw.isEmpty() ?  new Writable[0] : lw.toArray(new Writable[lw.size()]));
		Class<? extends Writable> type = (lw.isEmpty() ? NullWritable.get().getClass() : lw.get(0).getClass());
		return new ArrayWritable(type, values);
	}

	@Override
	protected Map<?, ?> createMap() {
		return new MapWritable();
	}
	
	@Override
	protected Object fieldName(String name) {
		return new Text(name);
	}

	@Override
	protected Object intValue(Parser parser) {
		return new IntWritable(parser.intValue());
	}

	@Override
	protected Object binaryValue(Parser parser) {
		return new BytesWritable(parser.binaryValue());
	}

	@Override
	protected Object booleanValue(Parser parser) {
		return new BooleanWritable(parser.booleanValue());
	}

	@Override
	protected Object doubleValue(Parser parser) {
		return new DoubleWritable(parser.doubleValue());
	}

	@Override
	protected Object floatVale(Parser parser) {
		return new FloatWritable(parser.floatValue());
	}

	@Override
	protected Object longValue(Parser parser) {
		return new LongWritable(parser.longValue());
	}

	@Override
	protected Object nullValue(Parser parser) {
		return NullWritable.get();
	}

	@Override
	protected Object textValue(Parser parser) {
		return new Text(parser.text());
	}
}