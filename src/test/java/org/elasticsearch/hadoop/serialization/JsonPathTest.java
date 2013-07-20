package org.elasticsearch.hadoop.serialization;

import java.io.InputStream;

import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.serialization.Parser.Token;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class JsonPathTest {

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
	public void testNoPath() throws Exception {
		ContentConsumer.seekToken(null, parser);
		assertNull(parser.currentToken());
		ContentConsumer.seekToken("", parser);
		assertNull(parser.currentToken());
		ContentConsumer.seekToken(" ", parser);
		assertNull(parser.currentToken());
	}

	@Test
	public void testNonExistingToken() throws Exception {
		ContentConsumer.seekToken("nosuchtoken", parser);
		assertNull(parser.currentToken());
		assertNull(parser.nextToken());
	}

	@Test
	public void testFieldName() throws Exception {
		ContentConsumer.seekToken("age", parser);
		assertEquals(Token.FIELD_NAME, parser.currentToken());
		assertEquals("age", parser.currentName());
	}

	@Test
	public void testOneLevelNestedField() throws Exception {
		ContentConsumer.seekToken("address/state", parser);
		assertEquals(Token.FIELD_NAME, parser.currentToken());
		assertEquals("state", parser.currentName());
	}
	@Test
	public void testFieldNestedButNotOnFirstLevel() throws Exception {
		ContentConsumer.seekToken("state", parser);
		System.out.println(parser.currentName());
		assertNull(parser.nextToken());
		assertNull(parser.currentToken());
	}
}