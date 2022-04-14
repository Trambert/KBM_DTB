# Databricks notebook source
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
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Reading a Stream</h2>
# MAGIC 
# MAGIC The method `SparkSession.readStream` returns a `DataStreamReader` used to configure the stream.
# MAGIC 
# MAGIC There are a number of key points to the configuration of a `DataStreamReader`:
# MAGIC * The schema
# MAGIC * The type of stream: Files, Kafka, TCP/IP, etc
# MAGIC * Configuration specific to the type of stream
# MAGIC   * For files, the file type, the path to the files, max files, etc...
# MAGIC   * For TCP/IP the server's address, port number, etc...
# MAGIC   * For Kafka the server's address, port, topics, partitions, etc...

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### The Schema
# MAGIC 
# MAGIC Every streaming DataFrame must have a schema - the definition of column names and data types.
# MAGIC 
# MAGIC Some sources such as Pub/Sub sources like Kafka and Event Hubs define the schema for you.
# MAGIC 
# MAGIC For file-based streaming sources, the schema must be user-defined.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why must a schema be specified for a streaming DataFrame?
# MAGIC 
# MAGIC To say that another way...
# MAGIC 
# MAGIC ### Why are streaming DataFrames unable to infer/read a schema?
# MAGIC 
# MAGIC If you have enough data, you can infer the schema.
# MAGIC <br><br>
# MAGIC If you don't have enough data you run the risk of miss-inferring the schema.
# MAGIC <br><br>
# MAGIC For example, you think you have all integers but the last value contains "1.123" (a float) or "snoopy" (a string).
# MAGIC <br><br>
# MAGIC With a stream, we have to assume we don't have enough data because we are starting with zero records.
# MAGIC <br><br>
# MAGIC And unlike reading from a table or parquet file, there is nowhere from which to "read" the stream's schema.
# MAGIC <br><br>
# MAGIC For this reason, we must specify the schema manually.

# COMMAND ----------

# Here we define the schema using a DDL-formatted string (the SQL Data Definition Language).
dataSchema = "Recorded_At timestamp, Device string, Index long, Model string, User string, _corrupt_record String, gt string, x double, y double, z double"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuring a File Stream
# MAGIC 
# MAGIC In our example below, we will be consuming files written continuously to a pre-defined directory.
# MAGIC 
# MAGIC To control how much data is pulled into Spark at once, we can specify the option `maxFilesPerTrigger`.
# MAGIC 
# MAGIC In our example below, we will be reading in only one file for every trigger interval:
# MAGIC 
# MAGIC `.option("maxFilesPerTrigger", 1)`
# MAGIC 
# MAGIC Both the location and file type are specified with the following call, which itself returns a `DataFrame`:
# MAGIC 
# MAGIC `.json(dataPath)`

# COMMAND ----------

dataPath = "dbfs:/mnt/training/definitive-guide/data/activity-data-stream.json"
initialDF = (spark
  .readStream                            # Returns DataStreamReader
  .option("maxFilesPerTrigger", 1)       # Force processing of only 1 file per trigger
  .schema(dataSchema)                    # Required for all streaming DataFrames
  .json(dataPath)                        # The stream's source directory and file type
)

# COMMAND ----------

# MAGIC %md
# MAGIC And with the initial `DataFrame`, we can apply some transformations:

# COMMAND ----------

streamingDF = (initialDF
  .withColumnRenamed("Index", "User_ID")  # Pick a "better" column name
  .drop("_corrupt_record")                # Remove an unnecessary column
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming DataFrames
# MAGIC 
# MAGIC Other than the call to `spark.readStream`, it looks just like any other `DataFrame`.
# MAGIC 
# MAGIC But is it a "streaming" `DataFrame`?
# MAGIC 
# MAGIC You can differentiate between a "static" and "streaming" `DataFrame` with the following call:

# COMMAND ----------

# Static vs Streaming?
streamingDF.isStreaming

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unsupported Operations
# MAGIC 
# MAGIC Most operations on a "streaming" DataFrame are identical to a "static" DataFrame.
# MAGIC 
# MAGIC There are some exceptions to this.
# MAGIC 
# MAGIC One such example would be to sort our never-ending stream by `Recorded_At`.

# COMMAND ----------

from pyspark.sql.functions import col

try:
  sortedDF = streamingDF.orderBy(col("Recorded_At").desc())
  display(sortedDF)
except:
  print("Sorting is not supported on an unaggregated stream")

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting is one of a handful of operations that is either too complex or logically not possible to do with a stream.
# MAGIC 
# MAGIC For more information on this topic, see the <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations" target="_blank">Structured Streaming Programming Guide / Unsupported Operations</a>.
# MAGIC 
# MAGIC > We will see in the following module how we can sort an **aggregated** stream.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Writing a Stream</h2>
# MAGIC 
# MAGIC The method `DataFrame.writeStream` returns a `DataStreamWriter` used to configure the output of the stream.
# MAGIC 
# MAGIC There are a number of parameters to the `DataStreamWriter` configuration:
# MAGIC * Query's name (optional) - This name must be unique among all the currently active queries in the associated SQLContext.
# MAGIC * Trigger (optional) - Default value is `ProcessingTime(0`) and it will run the query as fast as possible.
# MAGIC * Checkpointing directory (optional for pub/sub sinks)
# MAGIC * Output mode
# MAGIC * Output sink
# MAGIC * Configuration specific to the output sink, such as:
# MAGIC   * The host, port and topic of the receiving Kafka server
# MAGIC   * The file format and final destination of files
# MAGIC   * A <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=foreach#pyspark.sql.streaming.DataStreamWriter.foreach"target="_blank">custom sink</a> via `writeStream.foreach(...)`
# MAGIC 
# MAGIC Once the configuration is completed, we can trigger the job with a call to `.start()`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Triggers
# MAGIC 
# MAGIC The trigger specifies when the system should process the next set of data.
# MAGIC 
# MAGIC | Trigger Type                           | Example | Notes |
# MAGIC |----------------------------------------|-----------|-------------|
# MAGIC | Unspecified                            |  | _DEFAULT_- The query will be executed as soon as the system has completed processing the previous query |
# MAGIC | Fixed interval micro-batches           | `.trigger(Trigger.ProcessingTime("6 hours"))` | The query will be executed in micro-batches and kicked off at the user-specified intervals |
# MAGIC | One-time micro-batch                   | `.trigger(Trigger.Once())` | The query will execute _only one_ micro-batch to process all the available data and then stop on its own |
# MAGIC | Continuous w/fixed checkpoint interval | `.trigger(Trigger.Continuous("1 second"))` | The query will be executed in a low-latency, <a href="http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#continuous-processing" target = "_blank">continuous processing mode</a>. _EXPERIMENTAL_ in 2.3.2 |
# MAGIC 
# MAGIC In the example below, you will be using a fixed interval of 3 seconds:
# MAGIC 
# MAGIC `.trigger(Trigger.ProcessingTime("3 seconds"))`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpointing
# MAGIC 
# MAGIC A <b>checkpoint</b> stores the current state of your streaming job to a reliable storage system such as Azure Blob Storage or HDFS. It does not store the state of your streaming job to the local file system of any node in your cluster.
# MAGIC 
# MAGIC Together with write ahead logs, a terminated stream can be restarted and it will continue from where it left off.
# MAGIC 
# MAGIC To enable this feature, you only need to specify the location of a checkpoint directory:
# MAGIC 
# MAGIC `.option("checkpointLocation", checkpointPath)`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Points to consider:
# MAGIC * If you do not have a checkpoint directory, when the streaming job stops, you lose all state around your streaming job and upon restart, you start from scratch.
# MAGIC * For some sinks, you will get an error if you do not specify a checkpoint directory:<br/>
# MAGIC `analysisException: 'checkpointLocation must be specified either through option("checkpointLocation", ...)..`
# MAGIC * Also note that every streaming job should have its own checkpoint directory: no sharing.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Output Modes
# MAGIC 
# MAGIC | Mode   | Example | Notes |
# MAGIC | ------------- | ----------- |
# MAGIC | **Complete** | `.outputMode("complete")` | The entire updated Result Table is written to the sink. The individual sink implementation decides how to handle writing the entire table. |
# MAGIC | **Append** | `.outputMode("append")`     | Only the new rows appended to the Result Table since the last trigger are written to the sink. |
# MAGIC | **Update** | `.outputMode("update")`     | Only the rows in the Result Table that were updated since the last trigger will be outputted to the sink. Since Spark 2.1.1 |
# MAGIC 
# MAGIC In the example below, we are writing to a Parquet directory which only supports the `append` mode:
# MAGIC 
# MAGIC `dsw.outputMode("append")`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Output Sinks
# MAGIC 
# MAGIC `DataStreamWriter.format` accepts the following values, among others:
# MAGIC 
# MAGIC | Output Sink | Example                                          | Notes |
# MAGIC | ----------- | ------------------------------------------------ | ----- |
# MAGIC | **File**    | `dsw.format("parquet")`, `dsw.format("csv")`...  | Dumps the Result Table to a file. Supports Parquet, json, csv, etc.|
# MAGIC | **Kafka**   | `dsw.format("kafka")`      | Writes the output to one or more topics in Kafka |
# MAGIC | **Console** | `dsw.format("console")`    | Prints data to the console (useful for debugging) |
# MAGIC | **Memory**  | `dsw.format("memory")`     | Updates an in-memory table, which can be queried through Spark SQL or the DataFrame API |
# MAGIC | **foreach** | `dsw.foreach(writer: ForeachWriter)` | This is your "escape hatch", allowing you to write your own type of sink. |
# MAGIC | **Delta**    | `dsw.format("delta")`     | A proprietary sink |
# MAGIC 
# MAGIC In the example below, we will be appending files to a Parquet directory and specifying its location with this call:
# MAGIC 
# MAGIC `.format("parquet").start(outputPathDir)`

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Let's Do Some Streaming</h2>
# MAGIC 
# MAGIC In the cell below, we write data from a streaming query to `outputPathDir`.
# MAGIC 
# MAGIC There are a couple of things to note below:
# MAGIC 0. We are giving the query a name via the call to `.queryName` 
# MAGIC 0. Spark begins running jobs once we call `.start`
# MAGIC 0. The call to `.start` returns a `StreamingQuery` object

# COMMAND ----------

basePath = userhome + "/structured-streaming-concepts/python" # A working directory for our streaming app
dbutils.fs.mkdirs(basePath)                                   # Make sure that our working directory exists
outputPathDir = basePath + "/output.parquet"                  # A subdirectory for our output
checkpointPath = basePath + "/checkpoint"                     # A subdirectory for our checkpoint & W-A logs

streamingQuery = (streamingDF                                 # Start with our "streaming" DataFrame
  .writeStream                                                # Get the DataStreamWriter
  .queryName("stream_1p")                                     # Name the query
  .trigger(processingTime="3 seconds")                        # Configure for a 3-second micro-batch
  .format("parquet")                                          # Specify the sink type, a Parquet file
  .option("checkpointLocation", checkpointPath)               # Specify the location of checkpoint files & W-A logs
  .outputMode("append")                                       # Write only new data to the "file"
  .start(outputPathDir)                                       # Start the job, writing to the specified directory
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Managing Streaming Queries</h2>
# MAGIC 
# MAGIC When a query is started, the `StreamingQuery` object can be used to monitor and manage the query.
# MAGIC 
# MAGIC | Method    |  Description |
# MAGIC | ----------- | ------------------------------- |
# MAGIC |`id`| get unique identifier of the running query that persists across restarts from checkpoint data |
# MAGIC |`runId`| get unique id of this run of the query, which will be generated at every start/restart |
# MAGIC |`name`| get name of the auto-generated or user-specified name |
# MAGIC |`explain()`| print detailed explanations of the query |
# MAGIC |`stop()`| stop query |
# MAGIC |`awaitTermination()`| block until query is terminated, with stop() or with error |
# MAGIC |`exception`| exception if query terminated with error |
# MAGIC |`recentProgress`| array of most recent progress updates for this query |
# MAGIC |`lastProgress`| most recent progress update of this streaming query |

# COMMAND ----------

streamingQuery.recentProgress

# COMMAND ----------

# MAGIC %md
# MAGIC Additionally, we can iterate over a list of active streams:

# COMMAND ----------

for s in spark.streams.active:         # Iterate over all streams
  print("{}: {}".format(s.id, s.name)) # Print the stream's id and name

# COMMAND ----------

# MAGIC %md
# MAGIC The code below stops the `streamingQuery` defined above and introduces `awaitTermination()`
# MAGIC 
# MAGIC `awaitTermination()` will block the current thread
# MAGIC * Until the stream stops or
# MAGIC * Until the specified timeout elapses

# COMMAND ----------

streamingQuery.awaitTermination(5)      # Stream for another 5 seconds while the current thread blocks
streamingQuery.stop()                   # Stop the stream

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Display function</h2>
# MAGIC 
# MAGIC Within the Databricks notebooks, we can use the `display()` function to render a live plot
# MAGIC 
# MAGIC When you pass a "streaming" `DataFrame` to `display()`:
# MAGIC * A "memory" sink is being used
# MAGIC * The output mode is complete
# MAGIC * The query name is specified with the `streamName` parameter
# MAGIC * The trigger is specified with the `trigger` parameter
# MAGIC * The checkpointing location is specified with the `checkpointLocation`
# MAGIC 
# MAGIC `display(myDF, streamName = "myQuery")`
# MAGIC 
# MAGIC > We just programmatically stopped our only streaming query in the previous cell. In the cell below, `display` will automatically start our streaming DataFrame, `streamingDF`.  We are passing `stream_2p` as the name for this newly started stream.

# COMMAND ----------

myStream = "stream_2p"
display(streamingDF, streamName = myStream)

# COMMAND ----------

# MAGIC %md
# MAGIC Using the value passed to `streamName` in the call to `display`, we can programatically access this specific stream:

# COMMAND ----------

print("Looking for {}".format(myStream))

for stream in spark.streams.active:      # Loop over all active streams
  if stream.name == myStream:            # Single out "streamWithTimestamp"
    print("Found {} ({})".format(stream.name, stream.id))

# COMMAND ----------

# MAGIC %md
# MAGIC Since the `streamName` get's registered as a temporary table pointing to the memory sink, we can use SQL to query the sink.

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %md
# MAGIC Stop all remaining streams.

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC Clean up our directories

# COMMAND ----------

dbutils.fs.rm(basePath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## End-to-end Fault Tolerance
# MAGIC 
# MAGIC Structured Streaming ensures end-to-end exactly-once fault-tolerance guarantees through _checkpointing_ and <a href="https://en.wikipedia.org/wiki/Write-ahead_logging" target="_blank">Write Ahead Logs</a>.
# MAGIC 
# MAGIC Structured Streaming sources, sinks, and the underlying execution engine work together to track the progress of stream processing. If a failure occurs, the streaming engine attempts to restart and/or reprocess the data.
# MAGIC For best practices on recovering from a failed streaming query see <a href="">docs</a>.
# MAGIC 
# MAGIC This approach _only_ works if the streaming source is replayable. To ensure fault-tolerance, Structured Streaming assumes that every streaming source has offsets, akin to:
# MAGIC 
# MAGIC * <a target="_blank" href="https://kafka.apache.org/documentation/#intro_topics">Kafka message offsets</a>
# MAGIC * <a href="https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features" target="_blank"> Event Hubs offsets</a>
# MAGIC 
# MAGIC 
# MAGIC At a high level, the underlying streaming mechanism relies on a couple approaches:
# MAGIC 
# MAGIC * First, Structured Streaming uses checkpointing and write-ahead logs to record the offset range of data being processed during each trigger interval.
# MAGIC * Next, the streaming sinks are designed to be _idempotent_—that is, multiple writes of the same data (as identified by the offset) do _not_ result in duplicates being written to the sink.
# MAGIC 
# MAGIC Taken together, replayable data sources and idempotent sinks allow Structured Streaming to ensure **end-to-end, exactly-once semantics** under any failure condition.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Summary</h2>
# MAGIC 
# MAGIC We use `readStream` to read streaming input from a variety of input sources and create a DataFrame.
# MAGIC 
# MAGIC Nothing happens until we invoke `writeStream` or `display`.
# MAGIC 
# MAGIC Using `writeStream` we can write to a variety of output sinks. Using `display` we draw LIVE bar graphs, charts and other plot types in the notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Review Questions</h2>
# MAGIC 
# MAGIC **Q:** What do `readStream` and `writeStream` do?<br>
# MAGIC **A:** `readStream` creates a streaming DataFrame.<br>`writeStream` sends streaming data to a directory or other type of output sink.
# MAGIC 
# MAGIC **Q:** What does `display` output if it is applied to a DataFrame created via `readStream`?<br>
# MAGIC **A:** `display` sends streaming data to a LIVE graph!
# MAGIC 
# MAGIC **Q:** When you do a write stream command, what does this option do `outputMode("append")` ?<br>
# MAGIC **A:** This option takes on the following values and their respective meanings:
# MAGIC * <b>append</b>: add only new records to output sink
# MAGIC * <b>complete</b>: rewrite full output - applicable to aggregations operations
# MAGIC * <b>update</b>: update changed records in place
# MAGIC 
# MAGIC **Q:** What happens if you do not specify `option("checkpointLocation", pointer-to-checkpoint directory)`?<br>
# MAGIC **A:** When the streaming job stops, you lose all state around your streaming job and upon restart, you start from scratch.
# MAGIC 
# MAGIC **Q:** How do you view the list of active streams?<br>
# MAGIC **A:** Invoke `spark.streams.active`.
# MAGIC 
# MAGIC **Q:** How do you verify whether `streamingQuery` is running (boolean output)?<br>
# MAGIC **A:** Invoke `spark.streams.get(streamingQuery.id).isActive`.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Additional Topics &amp; Resources</h2>
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#" target="_blank">Structured Streaming Programming Guide</a>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tathagata Das. This is an excellent video describing how Structured Streaming works.
# MAGIC * <a href="https://docs.databricks.com/spark/latest/structured-streaming/production.html#id2" target="_blank">Failed Streaming Query Recovery</a> Best Practices for Recovery.
# MAGIC * <a href="https://databricks.com/blog/2018/03/20/low-latency-continuous-processing-mode-in-structured-streaming-in-apache-spark-2-3-0.html" target="_blank">Continuous Processing Mode</a> Lowest possible latency stream processing.  Currently Experimental.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC 
# MAGIC Start the next lesson, [Working with Time Windows]($./2.Time-Windows)
