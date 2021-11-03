[![Maven Central][maven-badge]][maven-url]

[maven-badge]: https://maven-badges.herokuapp.com/maven-central/com.pancakedb/spark-pancake-db-connector_2.12/badge.svg?gav=true
[maven-url]: https://search.maven.org/artifact/com.pancakedb/spark-pancake-db-connector_2.12

# Usage

This connector allows you to
* streaming write to PancakeDB
* batch write to PancakeDB
* batch read from PancakeDB

It leverages native code to access the PancakeDB core library.
The connector should work out of the box for the most common
up-to-date
OS/architecture combinations, but if you need to build the native libraries
for your own, see [the client library docs](https://github.com/pancake-db/pancake-scala-client#Requirements).

In your `build.sbt` or equivalent,

```
libraryDependencies += "com.pancakedb" % "spark-pancake-db-connector_2.12" % "0.0.0-alpha.0"
```

## Scala Example

For some complete examples, check out
[the examples subfolder](https://github.com/pancake-db/spark-pancake-connector/tree/main/examples).

```
// READ
val myDataFrame = sparkSession
  .read
  .format("pancake")
  .option("host", host)
  .option("port", port)
  .option("table_name", tableName)
  .load()
  .select("col_0", "col_1", "col_2")
  
// STREAMING WRITE
streamingDataset
  .writeStream
  .format("pancake")
  .outputMode(OutputMode.Append())
  .option("checkpointLocation", "/tmp/spark-pancake-test")
  .option("host", host)
  .option("port", port)
  .option("table_name", tableName)
  .start()
  
// BATCH WRITE
batchDataset
  .write
  .format("pancake")
  .mode(SaveMode.Append)
  .option("host", host)
  .option("port", port)
  .option("table_name", tableName)
  .save()
```

## Options

All the PancakeDB-specific options are:
* `table_name: String`
* `host: String`
* `port: Int`

(the following only apply to writes)
* `write_batch_size: Int`
  * Defaults to 256, which is the max and should be the best option

See [Parameters.scala](https://github.com/pancake-db/spark-pancake-connector/blob/main/src/main/scala/com/pancakedb/spark/Parameters.scala).

## `select` and `where` Pushdown

The connector takes advantage of 2 things when reading in batch data:
* `select` clauses with specific column names allow it to read in only the data for
those columns.
* `where` clauses with `===`, `>`, `>=`, `<`, `<=` on partition columns allow
it to read in only the data that matches those filters.

All other filters are applied on the Spark side.
