package structuredstreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import static org.apache.spark.sql.functions.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

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

  static {
    // Set log level to WARN for streaming-related logs
    Logger.getLogger("org.apache.spark.sql.execution.streaming").setLevel(Level.WARN);
  }

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

  // Main method
  public static void main(String[] args) throws Exception {
    spark.sparkContext().setLogLevel("ERROR");
    readFromSocket();
  }
}
