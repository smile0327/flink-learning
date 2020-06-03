

CREATE TABLE basevoltage_source(
  id BIGINT,
  system_id VARCHAR,
  mrid VARCHAR,
  name VARCHAR,
  nomkv VARCHAR,
  last_refresh_time TIMESTAMP(3)
) WITH (
  'connector.type' = 'jdbc',
  'connector.url' = 'jdbc:mysql://10.172.246.234:3306/ems_jingmen?autoReconnect=true&zeroDateTimeBehavior=convertToNull&useSSL=false',
  'connector.table' = 'basevoltage',
  'connector.driver' = 'com.mysql.jdbc.Driver',
  'connector.username' = 'root',
  'connector.password' = 'bobandata123'
);

CREATE TABLE basevoltage_sink(
  id BIGINT,
  system_id VARCHAR,
  mrid VARCHAR,
  name VARCHAR,
  nomkv VARCHAR,
  last_refresh_time TIMESTAMP(3)
) WITH (
  'connector.type' = 'jdbc',
  'connector.url' = 'jdbc:mysql://10.172.246.234:3306/ems_jingmen_test?autoReconnect=true&zeroDateTimeBehavior=convertToNull&useSSL=false',
  'connector.table' = 'basevoltage',
  'connector.driver' = 'com.mysql.jdbc.Driver',
  'connector.username' = 'root',
  'connector.password' = 'bobandata123'
);

SELECT vl.system_id,vl.mrid,vl.name,vl.substation,vl.baseVoltage,bv.name,bv.nomkv
FROM BASEVOLTAGE_SOURCE  AS bv JOIN voltagelevel  AS vl  ON bv.mrid = vl.baseVoltage;

INSERT INTO basevoltage_sink
SELECT * FROM basevoltage_source;