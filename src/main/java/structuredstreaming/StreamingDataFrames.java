package structuredstreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import static common.Schemas.stocksSchema;
import static org.apache.spark.sql.functions.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.streaming.Trigger;

public class StreamingDataFrames {

  // Create SparkSession
  private static final SparkSession spark =
      SparkSession.builder()
          .appName("Streaming Data Frames")
          .master("local[2]")
          .config(
              "spark.driver.extraJavaOptions",
              "--add-opens java.base/java.nio=ALL-UNNAMED  --add-exports java.base/sun.nio.ch=ALL-UNNAMED")
          .config(
              "spark.executor.extraJavaOptions",
              "--add-opens java.base/java.nio=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED")
          .getOrCreate();

  // Reading data from a socket
  public static void readFromSocket() throws Exception {
    Dataset<Row> lines =
        spark
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 12345)
            .load();

    // Transformation
    Dataset<Row> shortLines = lines.filter(length(col("value")).lt(5));

    // Tell between a static vs a streaming DataFrame
    System.out.println(shortLines.isStreaming());

    // Writing the stream to the console
    StreamingQuery query = shortLines.writeStream().format("console").outputMode("append").start();

    // Await termination
    query.awaitTermination();
  }

  public static void readFromFiles() throws Exception {
    Dataset<Row> stocksDF =
        spark
            .readStream()
            .format("csv")
            .option("header", "false")
            .option("dateFormat", "MMM d yyyy")
            .schema(stocksSchema)
            .load("src/main/resources/data/stocks");

    stocksDF.writeStream().format("console").outputMode("append").start().awaitTermination();
  }

  public static void demoTriggers() throws Exception {
    Dataset<Row> lines =
        spark
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 12345)
            .load();

    // Write the stream to the console
    lines
        .writeStream()
        .format("console")
        .outputMode("append")
        .trigger(
            // Trigger.ProcessingTime("5 seconds") // every 5 seconds run the query
            // Trigger.Once() // single batch, then terminate
            Trigger.Continuous(
                "2 seconds") // experimental, every 2 sec create a batch with whatever you have
            )
        .start()
        .awaitTermination();
  }

  // Main method
  // Before starting the demo, start the Netcat server doing:
  // nc -lk 12345
  public static void main(String[] args) throws Exception {
    // Set log level to ERROR - avoid INFO messages every second
    spark.sparkContext().setLogLevel("ERROR");

    // readFromSocket();
    // readFromFiles();
    demoTriggers();
  }
}
