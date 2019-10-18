A = LOAD '/data/artists' USING PigStorage('\t') AS (number: chararray, name: chararray, uri: chararray, picture: chararray, timestamp: chararray, tag: chararray);

STORE A INTO 'qa_kerberos_pig_data' USING org.elasticsearch.hadoop.pig.EsStorage(
    'es.nodes = localhost:9500',
    'es.security.authentication = kerberos',
    'es.net.spnego.auth.elasticsearch.principal = HTTP/build.elastic.co@BUILD.ELASTIC.CO'
);