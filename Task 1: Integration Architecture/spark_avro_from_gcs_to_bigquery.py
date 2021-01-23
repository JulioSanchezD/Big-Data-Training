from pyspark.sql import SparkSession

PROJECT_ID = 'cedar-freedom-298301'
DATASET_NAME = 'dataset_viajes'
# Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.
BUCKET = "big-data-training-julio"

spark = SparkSession.builder.\
            appName("GCSFilesRead").\
            config("spark.jars.packages", "org.apache.spark:spark-avro_2.11:2.4.6").\
            getOrCreate()
            # config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.18.1").\ 
            # Couldn't import this jar from maven repository, downloaded the corresponding jar instead

# Configure Google Cloud Credentials
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/home/julio/Big Data Training-0eb5fa5fe673.json")

# Load data from GCS.
df = spark.read.format("avro").load(f"gs://{BUCKET}/test-dataset-2.avro-00000-of-00001")
# print(df.show(1))

# Saving the data to BigQuery
df.write \
  .format("bigquery") \
  .option("temporaryGcsBucket", BUCKET) \
  .save(f"{PROJECT_ID}:{DATASET_NAME}.avro_table_spark")

print("\n\nFinished succesfully!\n\n")