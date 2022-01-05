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

SELECT ad.num, ad.name, ad.url, ad.picture, from_unixtime(unix_timestamp()), ad.tag FROM artist_data ad LIMIT 10;

DROP TABLE IF EXISTS es_artist_data;

CREATE EXTERNAL TABLE IF NOT EXISTS es_artist_data (
  num STRING,
  name STRING,
  url STRING,
  picture STRING,
  ts TIMESTAMP,
  tag STRING)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
  'es.resource' = 'qa_kerberos_hive_data',
  'es.security.authentication' = 'kerberos',
  'es.net.spnego.auth.elasticsearch.principal' = 'HTTP/build.elastic.co@BUILD.ELASTIC.CO'
);

-- Create random timestamps up front since Hive's timestamp format differs from ISO8601
INSERT OVERWRITE TABLE es_artist_data
SELECT ad.num, ad.name, ad.url, ad.picture, from_unixtime(unix_timestamp()), ad.tag FROM artist_data ad;
