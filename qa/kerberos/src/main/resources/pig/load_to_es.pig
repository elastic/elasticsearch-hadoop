A = LOAD '/data/artists' USING PigStorage('\t') AS (number: chararray, name: chararray, uri: chararray, picture: chararray, timestamp: chararray, tag: chararray);

DUMP A;

STORE A INTO 'qa_kerberos_pig_data/_doc' USING org.elasticsearch.hadoop.pig.EsStorage('es.nodes = localhost:9500');