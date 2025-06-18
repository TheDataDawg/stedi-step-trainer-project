CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `serialnumber` string COMMENT 'from deserializer', 
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://my-spark-data-lake/step_trainer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Step Trainer Landing to Trusted', 
  'CreatedByJobRun'='jr_2c597a423df400e13b5459e4812ab6d5ef287d11823080565f076d9ae1833f9c', 
  'classification'='json')