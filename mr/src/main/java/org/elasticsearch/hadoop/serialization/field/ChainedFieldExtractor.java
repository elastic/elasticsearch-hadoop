package org.elasticsearch.hadoop.serialization.field;

import java.util.List;

/**
 * A chained field extractor tries attempts to extract a field using each of it's configured
 * extractors and returns the first one that returns a value.
 *
 * This is helpful for things like join fields on grandchild records: A record might have a join
 * field which has a parent configured. That would normally be the routing value, but if they have
 * another field where they set the routing explicitly (like if the document is a grandchild) then
 * that explicit routing field should be picked up first.
 */
public class ChainedFieldExtractor implements FieldExtractor {

    public enum NoValueHandler {
        NOT_FOUND(FieldExtractor.NOT_FOUND),
        SKIP(FieldExtractor.SKIP);

        private Object returnValue;

        NoValueHandler(Object returnValue) {
            this.returnValue = returnValue;
        }
    }

    private final List<FieldExtractor> chain;
    private final NoValueHandler notFoundResponse;

    public ChainedFieldExtractor(List<FieldExtractor> chain) {
        this(chain, NoValueHandler.NOT_FOUND);
    }

    public ChainedFieldExtractor(List<FieldExtractor> chain, NoValueHandler notFoundResponse) {
        this.chain = chain;
        this.notFoundResponse = notFoundResponse;
    }

    @Override
    public Object field(Object target) {
        if (chain.isEmpty()) {
            return notFoundResponse.returnValue;
        }

        // Return first extracted field encountered
        for (FieldExtractor fieldExtractor : chain) {
            if (fieldExtractor != null) {
                Object extracted = fieldExtractor.field(target);
                if (extracted != NOT_FOUND) {
                    return extracted;
                }
            }
        }

        // Didn't find anything
        return notFoundResponse.returnValue;
    }
}
