package org.elasticsearch.hadoop.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class ErrorExtractorTest {
	@Test
	public void extractErrorWithCause() {
		final Map<String, String> nestedCause = ImmutableMap.<String, String>builder()
				.put("type", "illegal_argument_exception")
				.put("reason", "Failed to parse value [not_analyzed] as only [true] or [false] are allowed.")
				.build();
		final Map<String, Object> cause = ImmutableMap.<String, Object>builder()
				.put("type", "illegal_argument_exception")
				.put("reason", "Could not convert [version.index] to boolean")
				.put("caused_by", nestedCause)
				.build();
		
		final ErrorExtractor extractor = new ErrorExtractor(EsMajorVersion.V_5_X);
		
		final EsHadoopException ex = extractor.extractErrorWithCause(cause);
		checkException(ex, cause);
		
	}
	@Test
	public void extractErrorV5() {
		final Map<String, String> nestedCause = ImmutableMap.<String, String>builder()
				.put("type", "illegal_argument_exception")
				.put("reason", "Failed to parse value [not_analyzed] as only [true] or [false] are allowed.")
				.build();
		final Map<String, Object> cause = ImmutableMap.<String, Object>builder()
				.put("type", "illegal_argument_exception")
				.put("reason", "Could not convert [version.index] to boolean")
				.put("caused_by", nestedCause)
				.build();
		
		final Map<String, Object> error = ImmutableMap.<String, Object>builder()
				.put("error", cause)
				.build();
		
		final ErrorExtractor extractor = new ErrorExtractor(EsMajorVersion.V_5_X);
		
		final EsHadoopException ex = extractor.extractError(error);
		checkException(ex, cause);
		
	}
	@Test
	public void extractErrorV2() {
		final Map<String, Object> cause = ImmutableMap.<String, Object>builder()
				.put("type", "illegal_argument_exception")
				.put("reason", "Could not convert [version.index] to boolean")
				.build();
		
		final Map<String, Object> error = ImmutableMap.<String, Object>builder()
				.put("error", cause)
				.build();
		
		final ErrorExtractor extractor = new ErrorExtractor(EsMajorVersion.V_2_X);
		
		final EsHadoopException ex = extractor.extractError(error);
		checkException(ex, cause);
		
	}
	@Test
	public void extractErrorV1() {
		final Map<String, Object> error = ImmutableMap.<String, Object>builder()
				.put("error", "UnKnown Issue")
				.build();
		
		final ErrorExtractor extractor = new ErrorExtractor(EsMajorVersion.V_1_X);
		
		final EsHadoopException ex = extractor.extractError(error);
		
		assertNotNull(ex);
		assertTrue(EsHadoopRemoteException.class.isAssignableFrom(ex.getClass()));
		assertEquals(error.get("error"), ex.getMessage());
		
	}
	
	@SuppressWarnings("unchecked")
	protected void checkException(Throwable ex, Map<String, ?> json) {
		assertNotNull(ex);
		assertTrue(EsHadoopRemoteException.class.isAssignableFrom(ex.getClass()));
		
		final EsHadoopRemoteException exRemote = (EsHadoopRemoteException)ex;
		
		assertEquals(json.get("type"), exRemote.getType());
		assertEquals(json.get("reason"), exRemote.getMessage());
		
		if(json.containsKey("caused_by")) {
			assertNotNull(exRemote.getCause());
			checkException(exRemote.getCause(), (Map<String, ?>) json.get("caused_by"));
		}
	}
}
