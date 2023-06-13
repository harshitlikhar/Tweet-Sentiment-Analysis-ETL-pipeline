import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark context and session
sc = SparkContext()
spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(sparkContext=sc)

# Get job parameters
args = getResolvedOptions(sys.argv, ['REDSHIFT_CONNECTION', 'REDSHIFT_TABLE', 's3_input'])  # Corrected quotation marks

# Read the input data from S3
source_data = spark.read.json(args['s3_input'])  # Used args['s3_input'] as the parameter

# Apply transformations to the data
transformed_data = source_data.select(
    col('created_at').cast('timestamp').alias('created_at'),
    col('user.screen_name').alias('screen_name'),
    col('text').alias('tweet_text')
)

# Convert DataFrame to DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    frame=transformed_data,
    database='your-database',
    table_name='your-table'
)

# Write the DynamicFrame to Redshift
glueContext.write_dynamic_frame.from_catalog(
    frame=dynamic_frame,
    catalog_connection=args['REDSHIFT_CONNECTION'],
    connection_options={
        'dbtable': args['REDSHIFT_TABLE'],
        'database': 'your-redshift-database'
    }
)

# Print job completion message
print('ETL job completed successfully.')
