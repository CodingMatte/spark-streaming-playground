package structuredstreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StreamingAggregations {

  // Create SparkSession
  private static final SparkSession spark =
      SparkSession.builder()
          .appName("Streaming Data Frames")
          .master("local[2]")
          .config(
              "spark.driver.extraJavaOptions",
              "--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED "
                  + "--add-opens java.base/java.nio=ALL-UNNAMED --add-exports java.base/java.nio=ALL-UNNAMED "
                  + "--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED")
          .config(
              "spark.executor.extraJavaOptions",
              "--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED "
                  + "--add-opens java.base/java.nio=ALL-UNNAMED --add-exports java.base/java.nio=ALL-UNNAMED "
                  + "--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED")
          .getOrCreate();

  public static void streamingCount() throws Exception {
    // Reading data from a socket
    Dataset<Row> lines =
        spark
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 12345)
            .load();

    // Simple Count
    Dataset<Row> lineCount = lines.selectExpr("count(*) as lineCount");

    // aggregations with distinct are not supported
    // otherwise Spark will need to keep track of EVERYTHING
    // and, since it is UNBOUNDED, it is not possible

    lineCount
        .writeStream()
        .format("console")
        .outputMode("complete") // append and update not supported on aggregations without watermark
        .start()
        .awaitTermination();
  }

  // Main method
  // Before starting the demo, start the Netcat server doing:
  // nc -lk 12345
  public static void main(String[] args) throws Exception {
    // Set log level to ERROR - avoid INFO messages every second
    spark.sparkContext().setLogLevel("ERROR");

    streamingCount();
  }
}
