# Databricks notebook source
# MAGIC %md
# MAGIC # Structured Streaming with Azure EventHubs 
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC This notebook will consumn data being published through an EventHub with the following schema:
# MAGIC 
# MAGIC - `Index`
# MAGIC - `Arrival_Time`
# MAGIC - `Creation_Time`
# MAGIC - `x`
# MAGIC - `y`
# MAGIC - `z`
# MAGIC - `User`
# MAGIC - `Model`
# MAGIC - `Device`
# MAGIC - `gt`
# MAGIC - `id`
# MAGIC - `geolocation`
# MAGIC 
# MAGIC ## Library Requirements
# MAGIC 
# MAGIC 1. the Maven library with coordinate `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18`
# MAGIC    - this allows Databricks `spark` session to communicate with an Event Hub
# MAGIC 2. the Python library `azure-eventhub`
# MAGIC    - this is allows the Python kernel to stream content to an Event Hub
# MAGIC 
# MAGIC The next cell walks you through installing the Maven library. A couple cells below that, we automatically install the Python library using `%pip install`.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Lab Setup
# MAGIC 
# MAGIC If you are running in an Azure Databricks environment that is already pre-configured with the libraries you need, you can skip to the next cell. To use this notebook in your own Databricks environment, you will need to create libraries, using the [Create Library](https://docs.azuredatabricks.net/user-guide/libraries.html) interface in Azure Databricks. Follow the steps below to attach the `azure-eventhubs-spark` library to your cluster:
# MAGIC 
# MAGIC 1. In the left-hand navigation menu of your Databricks workspace, select **Compute**, then select your cluster in the list. If it's not running, start it now.
# MAGIC 
# MAGIC   ![Select cluster](https://databricksdemostore.blob.core.windows.net/images/10-de-learning-path/select-cluster.png)
# MAGIC 
# MAGIC 2. Select the **Libraries** tab (1), then select **Install New** (2). In the Install Library dialog, select **Maven** under Library Source (3). Under Coordinates, paste **com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18** (4), then select **Install**.
# MAGIC   
# MAGIC   ![Databricks new Maven library](https://databricksdemostore.blob.core.windows.net/images/10-de-learning-path/install-eventhubs-spark-library.png)
# MAGIC 
# MAGIC 3. Wait until the library successfully installs before continuing.
# MAGIC 
# MAGIC   ![Library installed](https://databricksdemostore.blob.core.windows.net/images/10-de-learning-path/eventhubs-spark-library-installed.png)
# MAGIC 
# MAGIC Once complete, return to this notebook to continue with the lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following two cells to install the `azure-eventhub` Python library and configure our "classroom."

# COMMAND ----------

# This library allows the Python kernel to stream content to an Event Hub:
%pip install azure-eventhub

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell sets up a local streaming file read that we'll be writing to Event Hubs.

# COMMAND ----------

# MAGIC %run ./Includes/Streaming-Demo-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC In order to reach Event Hubs, you will need to insert the connection string-primary key you acquired at the end of the Getting Started notebook in this module. You acquired this from the Azure Portal, and copied it into Notepad.exe or another text editor.
# MAGIC 
# MAGIC > Read this article to learn [how to acquire the connection string for an Event Hub](https://docs.microsoft.com/azure/event-hubs/event-hubs-create) in your own Azure Subscription.

# COMMAND ----------

event_hub_connection_string = "REPLACE-WITH-YOUR-EVENT-HUBS-CONNECTION-STRING" # Paste your Event Hubs connection string in the quotes to the left

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Azure Event Hubs</h2>
# MAGIC 
# MAGIC Microsoft Azure Event Hubs is a fully managed, real-time data ingestion service.
# MAGIC You can stream millions of events per second from any source to build dynamic data pipelines and immediately respond to business challenges.
# MAGIC It integrates seamlessly with a host of other Azure services.
# MAGIC 
# MAGIC Event Hubs can be used in a variety of applications such as
# MAGIC * Anomaly detection (fraud/outliers)
# MAGIC * Application logging
# MAGIC * Analytics pipelines, such as clickstreams
# MAGIC * Archiving data
# MAGIC * Transaction processing
# MAGIC * User telemetry processing
# MAGIC * Device telemetry streaming
# MAGIC * <b>Live dashboarding</b>

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC event_hub_name = "databricks-demo-eventhub"
# MAGIC connection_string = event_hub_connection_string + ";EntityPath=" + event_hub_name
# MAGIC 
# MAGIC print("Consumer Connection String: {}".format(connection_string))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream to Event Hub to Produce Stream

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.
# MAGIC ehWriteConf = {
# MAGIC   'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
# MAGIC }
# MAGIC 
# MAGIC checkpointPath = userhome + "/event-hub/write-checkpoint"
# MAGIC dbutils.fs.rm(checkpointPath,True)
# MAGIC 
# MAGIC (activityStreamDF
# MAGIC   .writeStream
# MAGIC   .format("eventhubs")
# MAGIC   .options(**ehWriteConf)
# MAGIC   .option("checkpointLocation", checkpointPath)
# MAGIC   .start())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Event Hubs Configuration</h2>
# MAGIC 
# MAGIC Assemble the following:
# MAGIC * A `startingEventPosition` as a JSON string
# MAGIC * An `EventHubsConf`
# MAGIC   * to include a string with connection credentials
# MAGIC   * to set a starting position for the stream read
# MAGIC   * to throttle Event Hubs' processing of the streams

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import json
# MAGIC 
# MAGIC # Create the starting position Dictionary
# MAGIC startingEventPosition = {
# MAGIC   "offset": "-1",
# MAGIC   "seqNo": -1,            # not in use
# MAGIC   "enqueuedTime": None,   # not in use
# MAGIC   "isInclusive": True
# MAGIC }
# MAGIC 
# MAGIC eventHubsConf = {
# MAGIC   "eventhubs.connectionString" : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
# MAGIC   "eventhubs.startingPosition" : json.dumps(startingEventPosition),
# MAGIC   "setMaxEventsPerTrigger": 100
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ### READ Stream using Event Hubs
# MAGIC 
# MAGIC The `readStream` method is a <b>transformation</b> that outputs a DataFrame with specific schema specified by `.schema()`.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import col
# MAGIC 
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
# MAGIC 
# MAGIC eventStreamDF = (spark.readStream
# MAGIC   .format("eventhubs")
# MAGIC   .options(**eventHubsConf)
# MAGIC   .load()
# MAGIC )
# MAGIC 
# MAGIC eventStreamDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Most of the fields in this response are metadata describing the state of the Event Hubs stream. We are specifically interested in the `body` field, which contains our JSON payload.
# MAGIC 
# MAGIC Noting that it's encoded as binary, as we select it, we'll cast it to a string.

# COMMAND ----------

# MAGIC %python
# MAGIC bodyDF = eventStreamDF.select(col("body").cast("STRING"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Each line of the streaming data becomes a row in the DataFrame once an <b>action</b> such as `writeStream` is invoked.
# MAGIC 
# MAGIC Notice that nothing happens until you engage an action, i.e. a `display()` or `writeStream`.

# COMMAND ----------

# MAGIC %python
# MAGIC display(bodyDF, streamName= "bodyDF")

# COMMAND ----------

# MAGIC %md
# MAGIC While we can see our JSON data now that it's cast to string type, we can't directly manipulate it.
# MAGIC 
# MAGIC Before proceeding, stop this stream. We'll continue building up transformations against this streaming DataFrame, and a new action will trigger an additional stream.

# COMMAND ----------

# MAGIC %python
# MAGIC for s in spark.streams.active:
# MAGIC   if s.name == "bodyDF":
# MAGIC     s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## <img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Parse the JSON payload
# MAGIC 
# MAGIC The Event Hub acts as a sort of "firehose" (or asynchronous buffer) and displays raw data in the JSON format.
# MAGIC 
# MAGIC If desired, we could save this as raw bytes or strings and parse these records further downstream in our processing.
# MAGIC 
# MAGIC Here, we'll directly parse our data so we can interact with the fields.
# MAGIC 
# MAGIC The first step is to define the schema for the JSON payload.
# MAGIC 
# MAGIC > Both time fields are encoded as `LongType` here because of non-standard formatting.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType
# MAGIC 
# MAGIC schema = StructType([
# MAGIC   StructField("Arrival_Time", LongType(), True),
# MAGIC   StructField("Creation_Time", LongType(), True),
# MAGIC   StructField("Device", StringType(), True),
# MAGIC   StructField("Index", LongType(), True),
# MAGIC   StructField("Model", StringType(), True),
# MAGIC   StructField("User", StringType(), True),
# MAGIC   StructField("gt", StringType(), True),
# MAGIC   StructField("x", DoubleType(), True),
# MAGIC   StructField("y", DoubleType(), True),
# MAGIC   StructField("z", DoubleType(), True),
# MAGIC   StructField("geolocation", StructType([
# MAGIC     StructField("PostalCode", StringType(), True),
# MAGIC     StructField("StateProvince", StringType(), True),
# MAGIC     StructField("city", StringType(), True),
# MAGIC     StructField("country", StringType(), True)
# MAGIC   ]), True),
# MAGIC   StructField("id", StringType(), True)
# MAGIC ])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Parse the data
# MAGIC 
# MAGIC Next we can use the function `from_json` to parse out the full message with the schema specified above.
# MAGIC 
# MAGIC When parsing a value from JSON, we end up with a single column containing a complex object.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import col, from_json
# MAGIC 
# MAGIC parsedEventsDF = bodyDF.select(
# MAGIC   from_json(col("body"), schema).alias("json"))
# MAGIC 
# MAGIC parsedEventsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we can further parse this to flatten the schema entirely and properly cast our time fields.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import from_unixtime
# MAGIC 
# MAGIC flatSchemaDF = (parsedEventsDF
# MAGIC   .select(from_unixtime(col("json.Arrival_Time")/1000).alias("Arrival_Time").cast("timestamp"),
# MAGIC           (col("json.Creation_Time")/1E9).alias("Creation_Time").cast("timestamp"),
# MAGIC           col("json.Device").alias("Device"),
# MAGIC           col("json.Index").alias("Index"),
# MAGIC           col("json.Model").alias("Model"),
# MAGIC           col("json.User").alias("User"),
# MAGIC           col("json.gt").alias("gt"),
# MAGIC           col("json.x").alias("x"),
# MAGIC           col("json.y").alias("y"),
# MAGIC           col("json.z").alias("z"),
# MAGIC           col("json.id").alias("id"),
# MAGIC           col("json.geolocation.country").alias("country"),
# MAGIC           col("json.geolocation.city").alias("city"),
# MAGIC           col("json.geolocation.PostalCode").alias("PostalCode"),
# MAGIC           col("json.geolocation.StateProvince").alias("StateProvince"))
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC This flat schema provides us the ability to view each nested field as a column.

# COMMAND ----------

# MAGIC %python
# MAGIC display(flatSchemaDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stop all active streams

# COMMAND ----------

# MAGIC %python
# MAGIC for s in spark.streams.active:
# MAGIC   s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Event Hubs FAQ
# MAGIC 
# MAGIC This [FAQ](https://github.com/Azure/azure-event-hubs-spark/blob/master/FAQ.md) can be an invaluable reference for occasional Spark-EventHub debugging.
