# Databricks notebook source
# MAGIC %md
# MAGIC ### DS 3002 - Data Project 2 (Course Capstone)
# MAGIC 
# MAGIC I created a data mart that represents an ecommerce business process. I used ecommerce data from the dbfs training data here on Databricks however I first downloaded it locally because I was intially using Jupyter Notebook for my code. The csv files I downloaded from that set I just converted over to json files which I then pushed towards MongoDB Atlas and then later to the SQL Server where I populated my data mart. My dimensional data mart consists of 3 dimensional tables - events., products, and users (+ the date dimension table). 
# MAGIC 
# MAGIC Although dimension tables are typically categorized as “slowly changing dimensions”, I chose to use the events dimension table to reflect real-time streaming data as there were many available instances of interactions between users and the ecommerce store. The orders/sales composes the fact table of the business process - listing the users, items bought as the transaction timestamp.
# MAGIC 
# MAGIC My data visualizations are attached as PNGs to the submission, I made a PivotTable on Excel and then used MongoDBs data visualization tool to display my data from the orders fact table.
# MAGIC 
# MAGIC **I ran into a number of technical issues during this project and tried my best to comment the code in which I ran into problems (these issues persisted even after consulting the resources emailed to the class and the troubleshooting office hours hosted this week).**

# COMMAND ----------

# Importing Required Libraries
import os
import json
import pymongo
import pyspark.pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DecimalType

# COMMAND ----------

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# COMMAND ----------

# Instatiating Global Variables
jdbcHostname = "ak-ds3002-sqlsvr.database.windows.net"
jdbcPort = 1433
srcDatabase = "ecommerce_dw"
dstDatabase = "ecommerce_dw"

connectionProperties = {
    "user" : "ayk2ea",
    "password" : "Password0!",
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

atlas_cluster_name = "sandbox"
atlas_dbname = "ecommerce_dw"
atlas_user_name = "ayk2ea"
atlas_password = "mongo"

data_dir = 'dbfs:/FileStore/'

outputPathBronze = data_dir + "/events/bronze"
outputPathSilver = data_dir + "/events/silver"
outputPathGold = data_dir + "/events/gold"

# COMMAND ----------

# Defining Global Functions

# ######################################################################################################################
# Use this Function to Fetch a DataFrame from the Azure SQL database server.
# ######################################################################################################################
def get_sql_dataframe(host_name, port, db_name, conn_props, sql_query):
    '''Create a JDBC URL to the Azure SQL Database'''
    jdbcUrl = f"jdbc:sqlserver://{host_name}:{port};database={db_name}"
    
    '''Invoke the spark.read.jdbc() function to query the database, and fill a Pandas DataFrame.'''
    dframe = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=conn_props)
    
    return dframe


# ######################################################################################################################
# Use this Function to Create New Collections by Uploading JSON file(s) to the MongoDB Atlas server.
# ######################################################################################################################
def set_mongo_collection(user_id, pwd, cluster_name, db_name, src_file_path, json_files):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://ayk2ea:mongo@Sandbox.zibbf.mongodb.net/ecommerce_dw?retryWrites=true&w=majority"
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    
    '''Read in a JSON file, and Use It to Create a New Collection'''
    for file in json_files:
        db.drop_collection(file)
        json_file = os.path.join(src_file_path, json_files[file])
        with open(json_file, 'r') as openfile:
            json_object = json.load(openfile)
            file = db[file]
            result = file.insert_many(json_object)

    client.close()
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC For this piece of code I kept running into authentication errors even though I was entering all my user/password information correct and troubleshooted many times. I somehow got the data to go in a collection through entering the same code in Jupyter Notebook, which I'll also attach in my final submission.

# COMMAND ----------

# Loading JSON Data in MongoDB Collection
src_dbname = "ecommerce_dw"
src_dir = '/dbfs/FileStore'
json_files = {"orders" : 'orders.json', "products" : 'products.json', "users" : 'users.json'}

set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, src_dbname, src_dir, json_files) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fact + Dimension tables brought in from MongoDB Atlas (orders, products, users)

# COMMAND ----------

# MAGIC %md
# MAGIC Ran into technical errors trying to reconnect with Azure SQL server after uploading DimDate, so I joined the data for the fact table in Jupyter Notebook... all these tables are in mySQL as well through code from Jupyter Notebook.

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.mongodb.spark._

# COMMAND ----------

# MAGIC %scala
# MAGIC val df_orders = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "ecommerce").option("collection", "orders").load()
# MAGIC display(df_orders)

# COMMAND ----------

# MAGIC %scala
# MAGIC val df_products = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "ecommerce").option("collection", "products").load()
# MAGIC display(df_products)

# COMMAND ----------

# MAGIC %scala
# MAGIC val df_users = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "ecommerce").option("collection", "users").load()
# MAGIC display(df_users)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Date Dimension brought in from SQL Server

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DimDate
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://ak-ds3002-sqlsvr.database.windows.net:1433;database=ecommerce-dw",
# MAGIC   dbtable "DimDate",
# MAGIC   user "ayk2ea",
# MAGIC   password "Password0!"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DimDate

# COMMAND ----------

# MAGIC %md
# MAGIC #### Streaming Data (events dimension table)

# COMMAND ----------

# MAGIC %run "/Users/ayk2ea@virginia.edu/04-Databricks/Includes/5.1-Lab-setup"

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# Define the schema using a DDL-formatted string.
dataSchema = "device string, ecommerce string, event_name string, event_previous_timestamp long, event_timestamp long, geo string, items string, traffic_source string, user_first_touch_timestamp long, user_id string"

# COMMAND ----------

outputPathDir = workingDir + "/output.parquet" # subdirectory for our output
checkpointPath = workingDir + "/checkpoint"    # subdirectory for our checkpoint & W-A logs
streamName = "events_ps"                       # arbitrary name for the stream

# COMMAND ----------

dataPath = "dbfs:/mnt/training/ecommerce/events/events-1m.json" # events-1m.json from dbfs ecommerce data
test = spark.read.json(dataPath)

display(test)

initialDF = (spark
  .readStream                            # returns DataStreamReader
  .option("maxFilesPerTrigger", 1)       # force processing of only 1 file per trigger 
  .schema(dataSchema)                    # required for all streaming DataFrames
  .json(dataPath)                        # the stream's source directory and file type
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Implementing bronze, silver and gold architecture
# MAGIC 
# MAGIC I tried several times to integrate the streaming data with my static/reference data but ran into technical errors I couldn't overcome so just settled to sort the streaming data by event type.

# COMMAND ----------

bronze = (initialDF
  .drop("_corrupt_record")                     # remove an unnecessary column
)

# COMMAND ----------

query = (bronze                                 # Start with our "streaming" DataFrame
  .writeStream                                  # Get the DataStreamWriter
  .queryName(streamName)                        # Name the query
  .trigger(processingTime="5 seconds")          # Configure for a 3-second micro-batch
  .format("parquet")                            # Specify the sink type, a Parquet file
  .option("checkpointLocation", checkpointPath) # Specify the location of checkpoint files & W-A logs
  .outputMode("append")                         # Write only new data to the "file"
  .start(outputPathDir)                         # Start the job, writing to the specified directory
)

# COMMAND ----------

untilStreamIsReady(streamName)                  # Wait until stream is done initializing...

# COMMAND ----------

newStreamName = 'bronze_temp'
display(bronze, streamName = newStreamName)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW bronze_enhanced_temp AS
# MAGIC SELECT
# MAGIC   *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM bronze_temp
# MAGIC   WHERE event_previous_timestamp > 0

# COMMAND ----------

silver_checkpoint_path = f"{DA.paths.checkpoints}/silver"

query = (spark.table("bronze_enhanced_temp")
              .writeStream
              .trigger(processingTime="5 seconds") 
              .format("delta")
              .option("checkpointLocation", silver_checkpoint_path)
              .outputMode("append")
              .table("silver")
              .start(outputPathDir))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

(spark
  .readStream
  .table("silver")
  .createOrReplaceTempView("silver_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_count_by_event_name AS
# MAGIC   SELECT event_name, count(event_name) AS customer_count
# MAGIC   FROM silver_temp
# MAGIC   GROUP BY event_name

# COMMAND ----------

customers_count_checkpoint_path = f"{DA.paths.checkpoints}/customers_counts"

query = (spark.table("customer_count_by_event_name_temp") # sorting by event name / type
              .writeStream
              .format("delta")
              .option("checkpointLocation", customers_count_checkpoint_path)
              .outputMode("complete")
              .table("gold_customer_count_by_event_name"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# Querying results
%sql
SELECT * FROM gold_customer_count_by_state

# COMMAND ----------

# Removing database + all data associated
DA.cleanup()

# COMMAND ----------

stopAllStreams()
