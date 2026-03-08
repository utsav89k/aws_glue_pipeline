import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Category_Source
Category_Source_node1772906448996 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-glue-uk/source_data/category/"], "recurse": True}, transformation_ctx="Category_Source_node1772906448996")

# Script generated for node Directors_Source
Directors_Source_node1772906449948 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-glue-uk/source_data/directors/"], "recurse": True}, transformation_ctx="Directors_Source_node1772906449948")

# Script generated for node Titles_Source
Titles_Source_node1772906449370 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-glue-uk/source_data/titles/"], "recurse": True}, transformation_ctx="Titles_Source_node1772906449370")

# Script generated for node Countries_Source
Countries_Source_node1772906448573 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-glue-uk/source_data/countries/"], "recurse": True}, transformation_ctx="Countries_Source_node1772906448573")

# Script generated for node Cast_Source
Cast_Source_node1772906447522 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-glue-uk/source_data/cast/"], "recurse": True}, transformation_ctx="Cast_Source_node1772906447522")

# Script generated for node S3 Category
S3Category_node1772907040930 = glueContext.write_dynamic_frame.from_options(frame=Category_Source_node1772906448996, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-glue-uk/bronze_data/category_bronze/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3Category_node1772907040930")

# Script generated for node Category Catalog
CategoryCatalog_node1772907036684 = glueContext.write_dynamic_frame.from_catalog(frame=Category_Source_node1772906448996, database="glue_project", table_name="category", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="CategoryCatalog_node1772907036684")

# Script generated for node S3 Director
S3Director_node1772907127848 = glueContext.write_dynamic_frame.from_options(frame=Directors_Source_node1772906449948, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-glue-uk/bronze_data/directors_bronze/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3Director_node1772907127848")

# Script generated for node Director Catalog
DirectorCatalog_node1772907123515 = glueContext.write_dynamic_frame.from_catalog(frame=Directors_Source_node1772906449948, database="glue_project", table_name="directors", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="DirectorCatalog_node1772907123515")

# Script generated for node S3 Titles
S3Titles_node1772907085763 = glueContext.write_dynamic_frame.from_options(frame=Titles_Source_node1772906449370, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-glue-uk/bronze_data/titles_bronze/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3Titles_node1772907085763")

# Script generated for node Titles Catalog
TitlesCatalog_node1772907082725 = glueContext.write_dynamic_frame.from_catalog(frame=Titles_Source_node1772906449370, database="glue_project", table_name="titles", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="TitlesCatalog_node1772907082725")

# Script generated for node S3 COUNTRIES
S3COUNTRIES_node1772906938214 = glueContext.write_dynamic_frame.from_options(frame=Countries_Source_node1772906448573, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-glue-uk/bronze_data/countries_bronze/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3COUNTRIES_node1772906938214")

# Script generated for node COUNTRIES Catalog
COUNTRIESCatalog_node1772907010830 = glueContext.write_dynamic_frame.from_catalog(frame=Countries_Source_node1772906448573, database="glue_project", table_name="countries", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="COUNTRIESCatalog_node1772907010830")

# Script generated for node CAST CATALOG
CASTCATALOG_node1772906665299 = glueContext.write_dynamic_frame.from_catalog(frame=Cast_Source_node1772906447522, database="glue_project", table_name="cast", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="CASTCATALOG_node1772906665299")

# Script generated for node Amazon S3 CAST
AmazonS3CAST_node1772906657215 = glueContext.write_dynamic_frame.from_options(frame=Cast_Source_node1772906447522, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-glue-uk/bronze_data/cast_bronze/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3CAST_node1772906657215")

job.commit()