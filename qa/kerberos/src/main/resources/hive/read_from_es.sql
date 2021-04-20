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

-- For some reason, creating and writing to an external table causes Hive to plan a reduce phase.
-- We can't use reduce phases with Kerberos security enabled because they require native libraries.
-- See Hadoop's SecureIOUtils static initialization for more information.
INSERT OVERWRITE DIRECTORY '/data/output/hive'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * FROM es_artist_data;
