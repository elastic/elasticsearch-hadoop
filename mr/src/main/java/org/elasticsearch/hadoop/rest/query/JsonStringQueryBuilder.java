package org.elasticsearch.hadoop.rest.query;

import org.elasticsearch.hadoop.serialization.Generator;

public class JsonStringQueryBuilder extends QueryBuilder {
    private final String jsonString;

    public JsonStringQueryBuilder(String jsonString) {
        this.jsonString = jsonString;
    }
    @Override
    public void toJson(Generator out) {

    }
}
