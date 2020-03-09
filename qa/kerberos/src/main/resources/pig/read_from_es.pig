A = LOAD 'qa_kerberos_pig_data' USING org.elasticsearch.hadoop.pig.EsStorage(
    'es.security.authentication = kerberos',
    'es.net.spnego.auth.elasticsearch.principal = HTTP/build.elastic.co@BUILD.ELASTIC.CO'
);

STORE A INTO '/data/output/pig' USING PigStorage('\t');