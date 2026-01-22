CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing (
  timeStamp BIGINT,
  user STRING,
  x DOUBLE,
  y DOUBLE,
  z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://cd0030bucket/accelerometer/'
TBLPROPERTIES ('has_encrypted_data'='false');
