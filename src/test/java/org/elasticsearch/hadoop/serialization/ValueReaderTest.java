package org.elasticsearch.hadoop.serialization;

import java.io.InputStream;

import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ValueReaderTest {
	
	private Parser parser;
	
	@Before
	public void before() {
		InputStream in = getClass().getResourceAsStream("parser-test.json");
		parser = new JacksonJsonParser(in);
	}

	@After
	public void after() {
		parser.close();
	}

	@Test
	public void testSimplePathReader() throws Exception {
		ContentConsumer consumer = ContentConsumer.consumer(parser, new SimpleValueReader());
		System.out.println(consumer.consume(Object.class));
		consumer.close();
	}
}
