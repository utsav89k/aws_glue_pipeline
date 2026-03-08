import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','Input_Path','Output_Path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Extracting the Path URL
input_url=args['Input_Path']
destination_url=args['Output_Path']

from pyspark.sql.functions import *
# Creating the OBT

# Reading the Silver Files
cast_df = spark.read.parquet(f"{input_url}/cast_silver/")
category_df = spark.read.parquet(f"{input_url}/category_silver/")
country_df = spark.read.parquet(f"{input_url}/countries_silver/")
director_df = spark.read.parquet(f"{input_url}/directors_silver/")
title_df = spark.read.parquet(f"{input_url}/titles_silver/")


df_joined = title_df.alias("title")\
              .join(cast_df.alias("cast"), col("title.show_id") == col("cast.show_id"), "left")\
              .join(category_df.alias("category"), col("title.show_id") == col("category.show_id"), "left")\
              .join(country_df.alias("country"), col("title.show_id") == col("country.show_id"), "left")\
              .join(director_df.alias("director"), col("title.show_id") == col("director.show_id"), "left")
              

# Selecting the columns for OBT
df_final = df_joined.select(
    col("title.show_id"),
    col("title.duration_minutes"),
    col("title.duration_seasons"),
    col("title.type"),
    col("title.title"),
    col("title.date_added"),
    col("title.release_year"),
    col("title.rating"),
    col("title.description"),
    col("title.ShortTitle"),
    col("title.flag_id"),
    col("title.duration_ranking"),
    col("cast.cast_name"),
    col("cast.First_name"),
    col("category.listed_in"),
    col("category.isTV"),
    col("country.country"),
    col("director.director"),
    col("director.Director_FirstName"),
    col("director.Director_LastName")
)

# Saving the Data
df_final.write.format("parquet")\
    .mode("append")\
    .option("path", destination_url)\
    .saveAsTable("glue_project.goldOBT")


job.commit()