CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing (
  sensorReadingTime BIGINT,
  serialNumber STRING,
  distanceFromObject DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://cd0030bucket/step_trainer/'
TBLPROPERTIES ('has_encrypted_data'='false');
