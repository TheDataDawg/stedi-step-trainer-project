CREATE EXTERNAL TABLE `machine_learning_curated`(
  `serialnumber` string COMMENT 'from deserializer', 
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer', 
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://my-spark-data-lake/machine_learning/curated/'
TBLPROPERTIES (
  'CreatedByJob'='Step Trainer and Accelerometer to ML Curated', 
  'CreatedByJobRun'='jr_3454055e5d1e751c1629f59be101cdc2ba2a4aae35e39a31aebcbd2844601b6e', 
  'classification'='json')