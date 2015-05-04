package org.elasticsearch.hadoop.serialization.builder;

// Optional interface used by readers interested in knowing when a document hit starts and ends
// and also when metadata is being parsed vs the document core
public interface ValueParsingCallback {

	void beginDoc();

	void beginLeadMetadata();

	void endLeadMetadata();

	void beginSource();
	
	void endSource();

	void beginTrailMetadata();
	
	void endTrailMetadata();
	
	void endDoc();
}
