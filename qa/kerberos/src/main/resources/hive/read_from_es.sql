DROP TABLE IF EXISTS es_artist_data;

CREATE EXTERNAL TABLE IF NOT EXISTS es_artist_data (
  num STRING,
  name STRING,
  url STRING,
  picture STRING,
  ts TIMESTAMP,
  tag STRING)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource' = 'qa_kerberos_hive_data');

DROP TABLE IF EXISTS artist_data;

CREATE EXTERNAL TABLE IF NOT EXISTS artist_data (
  num STRING,
  name STRING,
  url STRING,
  picture STRING,
  ts TIMESTAMP,
  tag STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/output/hive';

INSERT OVERWRITE TABLE artist_data SELECT * FROM es_artist_data;
