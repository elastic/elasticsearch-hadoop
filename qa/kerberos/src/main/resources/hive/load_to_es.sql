DROP TABLE IF EXISTS artist_data;

CREATE EXTERNAL TABLE IF NOT EXISTS artist_data (
  num STRING,
  name STRING,
  url STRING,
  picture STRING,
  ts STRING,
  tag STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/artists';

SELECT * FROM artist_data LIMIT 10;

DROP TABLE IF EXISTS es_artist_data;

CREATE EXTERNAL TABLE IF NOT EXISTS es_artist_data (
  num STRING,
  name STRING,
  url STRING,
  picture STRING,
  ts STRING,
  tag STRING)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
  'es.resource' = 'qa_kerberos_hive_data',
  'es.security.authentication' = 'kerberos',
  'es.net.spnego.auth.elasticsearch.principal' = 'HTTP/build.elastic.co@BUILD.ELASTIC.CO'
);

INSERT OVERWRITE TABLE es_artist_data
SELECT * FROM artist_data;
