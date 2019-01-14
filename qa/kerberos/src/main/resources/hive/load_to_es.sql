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
TBLPROPERTIES('es.resource' = 'qa_kerberos_hive_data/_doc', 'es.nodes' = 'localhost:9500');

INSERT OVERWRITE TABLE es_artist_data
SELECT * FROM artist_data;
