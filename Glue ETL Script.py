import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, upper, coalesce, lit
from awsglue.dynamicframe import DynamicFrame

## Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# --- Define S3 Paths (Updated with your new names) ---
s3_input_path = "s3://handsonfinallandingyamini/"
s3_processed_path = "s3://handsonfinalprocessedyamini/processed-data/"
s3_analytics_path = "s3://handsonfinalprocessedyamini/Athena Results/"

# --- Read the data from the S3 landing zone ---
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_input_path], "recurse": True},
    format="csv",
    format_options={"withHeader": True, "inferSchema": True},
)

# Convert to a standard Spark DataFrame for easier transformation
df = dynamic_frame.toDF()

# --- Perform Transformations ---
# 1. Cast 'rating' to integer and fill null values with 0
df_transformed = df.withColumn("rating", coalesce(col("rating").cast("integer"), lit(0)))

# 2. Convert 'review_date' string to a proper date type
df_transformed = df_transformed.withColumn("review_date", to_date(col("review_date"), "yyyy-MM-dd"))

# 3. Fill null review_text with a default string
df_transformed = df_transformed.withColumn("review_text",
    coalesce(col("review_text"), lit("No review text")))

# 4. Convert product_id to uppercase for consistency
df_transformed = df_transformed.withColumn("product_id_upper", upper(col("product_id")))


# --- Write the full transformed data to S3 (Good practice) ---
# This saves the clean, complete dataset to the 'processed-data' folder
glue_processed_frame = DynamicFrame.fromDF(df_transformed, glueContext, "transformed_df")
glueContext.write_dynamic_frame.from_options(
    frame=glue_processed_frame,
    connection_type="s3",
    connection_options={"path": s3_processed_path},
    format="csv"
)

# --- Run Spark SQL Query within the Job ---

# 1. Create a temporary view in Spark's memory
df_transformed.createOrReplaceTempView("product_reviews")

# 2. Run your SQL query
df_analytics_result = spark.sql("""
    SELECT 
        product_id_upper, 
        AVG(rating) as average_rating,
        COUNT(*) as review_count
    FROM product_reviews
    GROUP BY product_id_upper
    ORDER BY average_rating DESC
""")

# 3. Write the query's result DataFrame to your 'Athena Results' path
print(f"Writing analytics results to {s3_analytics_path}...")

# repartition(1) writes the result as a single file
analytics_result_frame = DynamicFrame.fromDF(df_analytics_result.repartition(1), glueContext, "analytics_df")
glueContext.write_dynamic_frame.from_options(
    frame=analytics_result_frame,
    connection_type="s3",
    connection_options={"path": s3_analytics_path},
    format="csv"
)

# Query 2: Date-wise review count
df_daily_counts = spark.sql("""
    SELECT 
        review_date,
        COUNT(*) as review_count
    FROM product_reviews
    GROUP BY review_date
    ORDER BY review_date ASC
""")

daily_counts_frame = DynamicFrame.fromDF(df_daily_counts.repartition(1), glueContext, "daily_counts_df")
glueContext.write_dynamic_frame.from_options(
    frame=daily_counts_frame,
    connection_type="s3",
    connection_options={"path": s3_analytics_path + "daily_review_counts/"},
    format="csv"
)

# Query 3: Top 5 Most Active Customers
df_top_customers = spark.sql("""
    SELECT 
        customer_id,
        COUNT(*) as total_reviews
    FROM product_reviews
    GROUP BY customer_id
    ORDER BY total_reviews DESC
    LIMIT 5
""")

top_customers_frame = DynamicFrame.fromDF(df_top_customers.repartition(1), glueContext, "top_customers_df")
glueContext.write_dynamic_frame.from_options(
    frame=top_customers_frame,
    connection_type="s3",
    connection_options={"path": s3_analytics_path + "top_5_customers/"},
    format="csv"
)

# Query 4: Overall Rating Distribution
df_rating_dist = spark.sql("""
    SELECT 
        rating,
        COUNT(*) as count
    FROM product_reviews
    WHERE rating IS NOT NULL AND rating != 0
    GROUP BY rating
    ORDER BY rating ASC
""")

rating_dist_frame = DynamicFrame.fromDF(df_rating_dist.repartition(1), glueContext, "rating_dist_df")
glueContext.write_dynamic_frame.from_options(
    frame=rating_dist_frame,
    connection_type="s3",
    connection_options={"path": s3_analytics_path + "rating_distribution/"},
    format="csv"
)

job.commit()
