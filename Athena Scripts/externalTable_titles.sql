CREATE EXTERNAL TABLE IF NOT EXISTS `glue_project`.`titles` (
  `duration_minutes` string,
  `duration_seasons` string,
  `type` string,
  `title` string,
  `date_added` string,
  `release_year` string,
  `rating` string,
  `description` string,
  `show_id` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://project-glue-uk/external_tables/titles/'
TBLPROPERTIES ('classification' = 'parquet');