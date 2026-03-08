CREATE EXTERNAL TABLE IF NOT EXISTS `glue_project`.`cast` (`cast` string, `show_id` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://project-glue-uk/external_tables/cast/'
TBLPROPERTIES ('classification' = 'parquet');