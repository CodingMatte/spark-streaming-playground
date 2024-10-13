package structuredstreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

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

    // Writing the stream to the console
    StreamingQuery query = lines.writeStream().format("console").outputMode("append").start();

    // Await termination
    query.awaitTermination();
  }

  // Main method
  public static void main(String[] args) throws Exception {
    readFromSocket();
  }
}
