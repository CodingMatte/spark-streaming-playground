package structuredstreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public class StreamingJoins {

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

  // Load the guitarPlayers dataset
  private static final Dataset guitarPlayers =
      spark.read().option("inferSchema", true).json("src/main/resources/data/guitarPlayers");

  // Load the guitars dataset
  private static final Dataset guitars =
      spark.read().option("inferSchema", true).json("src/main/resources/data/guitars");

  // Load the bands dataset
  private static final Dataset bands =
      spark.read().option("inferSchema", true).json("src/main/resources/data/bands");

  // joining static DFs
  private static final Column joinCondition = guitarPlayers.col("band").equalTo(bands.col("id"));
  private static final Dataset<Row> guitaristsBand =
      guitarPlayers.join(bands, joinCondition, "inner");
  private static final StructType bandsSchema = bands.schema();

  // joining Stream DF with Static DFs
  private static void joinStreamWithStatic() throws Exception {
    // Reading data from a socket
    Dataset<Row> streamedBandDF =
        spark
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 12345)
            .load() // a DF with a single column "value" of type String
            .select(from_json(col("value"), bandsSchema).as("band"))
            .selectExpr(
                "band.id as id",
                "band.name as name",
                "band.hometown as hometown",
                "band.year as year");

    // join happens per BATCH
    Dataset<Row> streamedBandsGuitaristsDF =
        streamedBandDF.join(
            guitarPlayers, guitarPlayers.col("band").equalTo(streamedBandDF.col("id")), "inner");

    /*
     restricted joins:
     - stream joining with static: RIGHT outer join/full outer join/right_semi not permitted
     - static joining with streaming: LEFT outer join/full/left_semi not permitted
    */

    // Write stream to console
    streamedBandsGuitaristsDF
        .writeStream()
        .format("console")
        .outputMode("append")
        .start()
        .awaitTermination();
  }

  // since Spark 2.3 we have stream vs stream joins
  public static void joinStreamWithStream() throws Exception {
    // Reading data from a socket
    Dataset<Row> streamedBandsDF =
        spark
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 12345)
            .load() // a DF with a single column "value" of type String
            .select(from_json(col("value"), bandsSchema).as("band"))
            .selectExpr(
                "band.id as id",
                "band.name as name",
                "band.hometown as hometown",
                "band.year as year");

    Dataset<Row> streamedGuitaristsDF =
        spark
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 12346)
            .load()
            .select(from_json(col("value"), guitarPlayers.schema()).as("guitarPlayer"))
            .selectExpr(
                "guitarPlayer.id as id",
                "guitarPlayer.name as name",
                "guitarPlayer.guitars as guitars",
                "guitarPlayer.band as band");

    // join stream with stream
    Dataset<Row> streamedJoin =
        streamedBandsDF.join(
            streamedGuitaristsDF,
            streamedGuitaristsDF.col("band").equalTo(streamedBandsDF.col("id")));

    /*
     - inner joins are supported
     - left/right outer joins ARE supported, but MUST have watermarks
     - full outer joins are NOT supported
    */

    streamedJoin
        .writeStream()
        .format("console")
        .outputMode("append") // only append supported for stream vs stream join
        .start()
        .awaitTermination();
  }

  public static void main(String[] args) throws Exception {
    spark.sparkContext().setLogLevel("ERROR");
    // joinStreamWithStatic();
    joinStreamWithStream();
  }
}
