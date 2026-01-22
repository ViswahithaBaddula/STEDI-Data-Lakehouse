CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
  serialnumber STRING,
  sharewithpublicasofdate BIGINT,
  birthday STRING,
  registrationdate BIGINT,
  sharewithresearchasofdate BIGINT,
  customername STRING,
  email STRING,
  lastupdatedate BIGINT,
  phone STRING,
  sharewithfriendsasofdate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://cd0030bucket/customers/'
TBLPROPERTIES ('has_encrypted_data'='false');
