# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Working with Time Windows
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Use sliding windows to aggregate over chunks of data rather than all data
# MAGIC * Apply watermarking to throw away stale old data that you do not have space to keep
# MAGIC * Plot live graphs using `display`
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Secondary Audience: Data Scientists, Software Engineers

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Getting Started</h2>
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Streaming Aggregations</h2>
# MAGIC 
# MAGIC Continuous applications often require near real-time decisions on real-time, aggregated statistics.
# MAGIC 
# MAGIC Some examples include
# MAGIC * Aggregating errors in data from IoT devices by type
# MAGIC * Detecting anomalous behavior in a server's log file by aggregating by country.
# MAGIC * Doing behavior analysis on instant messages via hash tags.
# MAGIC 
# MAGIC However, in the case of streams, you generally don't want to run aggregations over the entire dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ### What problems might you encounter if you aggregate over a stream's entire dataset?
# MAGIC 
# MAGIC While streams have a definitive start, there conceptually is no end to the flow of data.
# MAGIC 
# MAGIC Because there is no "end" to a stream, the size of the dataset grows in perpetuity.
# MAGIC 
# MAGIC This means that your cluster will eventually run out of resources.
# MAGIC 
# MAGIC Instead of aggregating over the entire dataset, you can aggregate over data grouped by windows of time (say, every 5 minutes or every hour).
# MAGIC 
# MAGIC This is referred to as windowing

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Windowing</h2>
# MAGIC 
# MAGIC If we were using a static DataFrame to produce an aggregate count, we could use `groupBy()` and `count()`.
# MAGIC 
# MAGIC Instead we accumulate counts within a sliding window, answering questions like "How many records are we getting every second?"
# MAGIC 
# MAGIC **Sliding windows** 
# MAGIC 
# MAGIC The windows overlap and a single event may be aggregated into multiple windows. 
# MAGIC 
# MAGIC **Tumbling Windows**
# MAGIC 
# MAGIC The windows do not overlap and a single event will be aggregated into only one window. 
# MAGIC 
# MAGIC The diagram below shows sliding windows. 
# MAGIC 
# MAGIC The following illustration, from the <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">Structured Streaming Programming Guide</a> guide, helps us understanding how it works:

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-window.png">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Event Time vs Receipt Time</h2>
# MAGIC 
# MAGIC **Event Time** is the time at which the event occurred in the real world.
# MAGIC 
# MAGIC **Event Time** is **NOT** something maintained by the Structured Streaming framework.
# MAGIC 
# MAGIC At best, Structured Streaming only knows about **Receipt Time** - the time a piece of data arrived in Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ### What are some examples of **Event Time**? **of Receipt Time**?
# MAGIC 
# MAGIC #### Examples of *Event Time*:
# MAGIC * The timestamp recorded in each record of a log file
# MAGIC * The instant at which an IoT device took a measurement
# MAGIC * The moment a REST API received a request
# MAGIC 
# MAGIC #### Examples of *Receipt Time*:
# MAGIC * A timestamp added to a DataFrame the moment it was processed by Spark
# MAGIC * The timestamp extracted from an hourly log file's file name
# MAGIC * The time at which an IoT hub received a report of a device's measurement
# MAGIC   - Presumably offset by some delay from when the measurement was taken

# COMMAND ----------

# MAGIC %md
# MAGIC ### What are some of the inherent problems with using **Receipt Time**?
# MAGIC 
# MAGIC The main problem with using **Receipt Time** is going to be with accuracy. For example:
# MAGIC 
# MAGIC * The time between when an IoT device takes a measurement vs when it is reported can be off by several minutes.
# MAGIC   - This could have significant ramifications to security and health devices, for example
# MAGIC * The timestamp embedded in an hourly log file can be off by up to one hour making correlations to other events extremely difficult
# MAGIC * The timestamp added by Spark as part of a DataFrame transformation can be off by hours to weeks to months depending on when the event occurred and when the job ran

# COMMAND ----------

# MAGIC %md
# MAGIC ### When might it be OK to use **Receipt Time** instead of **Event Time**?
# MAGIC 
# MAGIC When accuracy is not a significant concern - that is **Receipt Time** is close enough to **Event Time**
# MAGIC 
# MAGIC One example would be for IoT events that can be delayed by minutes but the resolution of your query is by days or months (close enough)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Windowed Streaming Example</h2>
# MAGIC 
# MAGIC For this example, we will examine the files in `/mnt/training/sensor-data/accelerometer/time-series-stream.json/`.
# MAGIC 
# MAGIC Each line in the file contains a JSON record with two fields: `time` and `action`
# MAGIC 
# MAGIC New files are being written to this directory continuously (aka streaming).
# MAGIC 
# MAGIC Theoretically, there is no end to this process.
# MAGIC 
# MAGIC Let's start by looking at the head of one such file:

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/training/sensor-data/accelerometer/time-series-stream.json/file-0.json

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Define the Schema for the streaming content
# MAGIC 
# MAGIC Let's try to analyze these files interactively.
# MAGIC 
# MAGIC First configure a schema.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The schema must be specified for file-based Structured Streams.
# MAGIC Because of the simplicity of the schema, we can use the simpler, DDL-formatted, string representation of the schema.

# COMMAND ----------

inputPath = "dbfs:/mnt/training/sensor-data/accelerometer/time-series-stream.json/"

jsonSchema = "time timestamp, action string"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Define a streaming Dataframe
# MAGIC 
# MAGIC With the schema defined, we can create the initial DataFrame `inputDf` and then `countsDF` which represents our aggregation:

# COMMAND ----------

from pyspark.sql.functions import window, col

inputDF = (spark
  .readStream                                 # Returns an instance of DataStreamReader
  .schema(jsonSchema)                         # Set the schema of the JSON data
  .option("maxFilesPerTrigger", 1)            # Treat a sequence of files as a stream, one file at a time
  .json(inputPath)                            # Specifies the format, path and returns a DataFrame
)

countsDF = (inputDF
  .groupBy(col("action"),                     # Aggregate by action...
           window(col("time"), "1 hour"))     # ...then by a 1 hour window
  .count()                                    # For the aggregate, produce a count
  .select(col("window.start").alias("start"), # Elevate field to column
          col("action"),                      # Include count
          col("count"))                       # Include action
  .orderBy(col("start"), col("action"))       # Sort by the start time and action
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### View Results
# MAGIC 
# MAGIC To view the results of our query, pass the DataFrame `countsDF` to the `display()` function.

# COMMAND ----------

display(countsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Performance Considerations
# MAGIC 
# MAGIC If you run that query, as is, it will take a surprisingly long time to start generating data. What's the cause of the delay?
# MAGIC 
# MAGIC If you expand the **Spark Jobs** component, you'll see something like this:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/structured-streaming-shuffle-partitions-200.png"/>
# MAGIC 
# MAGIC It's our `groupBy()`. `groupBy()` causes a _shuffle_, and, by default, Spark SQL shuffles to 200 partitions. In addition, we're doing a _stateful_ aggregation: one that requires Structured Streaming to maintain and aggregate data over time.
# MAGIC 
# MAGIC When doing a stateful aggregation, Structured Streaming must maintain an in-memory _state map_ for each window within each partition. For fault tolerance reasons, the state map has to be saved after a partition is processed, and it needs to be saved somewhere fault-tolerant. To meet those requirements, the Streaming API saves the maps to a distributed store. On some clusters, that will be HDFS. Azure Databricks uses the DBFS.
# MAGIC 
# MAGIC That means that every time it finishes processing a window, the Streaming API writes its internal map to disk. The write has some overhead, typically between 1 and 2 seconds.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### What's the cause of the delay?
# MAGIC * `groupBy()` causes a **shuffle**
# MAGIC * By default, this produces **200 partitions**
# MAGIC * Plus a **stateful aggregation** to be maintained **over time**
# MAGIC 
# MAGIC This results in :
# MAGIC * Maintenance of an **in-memory state map** for **each window** within **each partition**
# MAGIC * Writing of the state map to a fault-tolerant store
# MAGIC   * On some clusters, that will be HDFS
# MAGIC   * Azure Databricks uses the DBFS
# MAGIC * Around 1 to 2 seconds overhead

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Shuffle Partition Best Practices
# MAGIC 
# MAGIC One way to reduce this overhead is to reduce the number of partitions Spark shuffles to.
# MAGIC 
# MAGIC In most cases, you want a 1-to-1 mapping of partitions to cores for streaming applications.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Run query with proper setting for shuffle partitions
# MAGIC 
# MAGIC Rerun the query below and notice the performance improvement.
# MAGIC 
# MAGIC Once the data is loaded, render a line graph with
# MAGIC * **Keys** is set to `start`
# MAGIC * **Series groupings** is set to `action`
# MAGIC * **Values** is set to `count`

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

display(countsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stop all Streams
# MAGIC 
# MAGIC When you are done, stop all the streaming jobs.

# COMMAND ----------

for s in spark.streams.active: # Iterate over all active streams
  s.stop()                     # Stop the stream

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Problem with Generating Many Windows</h2>
# MAGIC 
# MAGIC We are generating a window for every 1 hour aggregate.
# MAGIC 
# MAGIC _Every window_ has to be separately persisted and maintained.
# MAGIC 
# MAGIC Over time, this aggregated data will build up in the driver.
# MAGIC 
# MAGIC The end result being a massive slowdown if not an OOM Error.
# MAGIC 
# MAGIC ### How do we fix that problem?
# MAGIC 
# MAGIC One simple solution is to increase the size of our window (say, to 2 hours).
# MAGIC 
# MAGIC That way, we're generating fewer windows.
# MAGIC 
# MAGIC But if the job runs for a long time, we're still building up an unbounded set of windows.
# MAGIC 
# MAGIC Eventually, we could hit resource limits.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Watermarking</h2>
# MAGIC 
# MAGIC A better solution to the problem is to define a cut-off.
# MAGIC 
# MAGIC A point after which Structured Streaming will commit windowed data to sink, or throw it away if the sink is console or memory as `display()` mimics.
# MAGIC 
# MAGIC That's what _watermarking_ allows us to do.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Refining our previous example
# MAGIC 
# MAGIC Below is our previous example with watermarking.
# MAGIC 
# MAGIC We're telling Structured Streaming to keep no more than 2 hours of aggregated data.

# COMMAND ----------

watermarkedDF = (inputDF
  .withWatermark("time", "2 hours")           # Specify a 2-hour watermark
  .groupBy(col("action"),                     # Aggregate by action...
           window(col("time"), "1 hour"))     # ...then by a 1 hour window
  .count()                                    # For each aggregate, produce a count
  .select(col("window.start").alias("start"), # Elevate field to column
          col("action"),                      # Include count
          col("count"))                       # Include action
  .orderBy(col("start"), col("action"))       # Sort by the start time
)
display(watermarkedDF)                        # Start the stream and display it

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Example Details
# MAGIC 
# MAGIC In the example above,
# MAGIC * Data received 2 hour _past_ the watermark will be dropped.
# MAGIC * Data received within 2 hours of the watermark will never be dropped.
# MAGIC 
# MAGIC More specifically, any data less than 2 hours behind the latest data processed till then is guaranteed to be aggregated.
# MAGIC 
# MAGIC However, the guarantee is strict only in one direction.
# MAGIC 
# MAGIC Data delayed by more than 2 hours is not guaranteed to be dropped; it may or may not get aggregated.
# MAGIC 
# MAGIC The more delayed the data is, the less likely the engine is going to process it.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stop all the streams

# COMMAND ----------

for s in spark.streams.active: # Iterate over all active streams
  s.stop()                     # Stop the stream


# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC 
# MAGIC Start the next lesson, [Structured Streaming with Azure EventHubs]($./3.Streaming-With-Event-Hubs-Demo)
