package advanced;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;
import utils.SparkUtils;

public class EventTimeWindows {
  // initialize spark session from spark utils
  private static final SparkUtils sparkUtils = new SparkUtils("Event Time Windows", "local[*]");
  private static final SparkSession spark = sparkUtils.getSparkSession();

  // --add-opens java.base/java.nio=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED

  private static final StructType onlinePurchaseSchema =
      new StructType(
          new StructField[] {
            new StructField("id", DataTypes.StringType, false, Metadata.empty()),
            new StructField("time", DataTypes.TimestampType, false, Metadata.empty()),
            new StructField("item", DataTypes.StringType, false, Metadata.empty()),
            new StructField("quantity", DataTypes.IntegerType, false, Metadata.empty())
          });

  private static Dataset<Row> readPurchasesFromSocket() {
    return spark
        .readStream()
        .format("socket")
        .option("host", "localhost")
        .option("port", 12345)
        .load()
        .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
        .selectExpr("purchase.*");
  }

  public static Dataset<Row> readPurchasesFromFile() {
    return spark
        .readStream()
        .schema(onlinePurchaseSchema)
        .json("src/main/resources/data/purchases");
  }

  public static void aggregatePurchasesByTumblingWindow() throws Exception {
    Dataset<Row> purchasesDF = readPurchasesFromSocket();

    Dataset<Row> windowByDay =
        purchasesDF
            .groupBy(
                window(col("time"), "1 day")
                    .as("time")) // tumbling window: sliding duration == window duration
            .agg(sum("quantity").as("totalQuantity"))
            .select(
                col("time").getField("start").as("start"),
                col("time").getField("end").as("end"),
                col("totalQuantity"));

    windowByDay
        .writeStream()
        .format("console")
        .outputMode("complete") // output mode Complete
        .start()
        .awaitTermination();
  }

  public static void aggregatePurchasesBySlidingWindow() throws Exception {
    Dataset<Row> purchasesDF = readPurchasesFromSocket();

    Dataset<Row> windowByDay =
        purchasesDF
            .groupBy(
                window(col("time"), "1 day", "1 hour")
                    .as("time")) // struct column: has fields {start, end}
            .agg(sum("quantity").as("totalQuantity"))
            .select(
                col("time").getField("start").as("start"),
                col("time").getField("end").as("end"),
                col("totalQuantity"));

    windowByDay
        .writeStream()
        .format("console")
        .outputMode("complete") // output mode Complete
        .start()
        .awaitTermination();
  }

  /**
   * Exercises 1) Show the best selling product of every day, + quantity sold. 2) Show the best
   * selling product of every 24 hours, updated every hour.
   */
  public static void bestSellingProductPerDay() throws Exception {
    Dataset<Row> purchasesDF = readPurchasesFromFile();

    Dataset<Row> bestSelling =
        purchasesDF
            .groupBy(
                col("item"),
                window(col("time"), "1 day").as("day")) // struct column: has fields {start, end}
            .agg(sum("quantity").as("totalQuantity"))
            .select(
                col("day").getField("start").as("start"),
                col("day").getField("end").as("end"),
                col("item"),
                col("totalQuantity"))
            .orderBy(col("day"), col("totalQuantity").desc());

    bestSelling
        .writeStream()
        .format("console")
        .outputMode("complete") // output mode Complete
        .start()
        .awaitTermination();
  }

  public static void bestSellingProductEvery24h() throws Exception {
    Dataset<Row> purchasesDF = readPurchasesFromFile();

    Dataset<Row> bestSelling =
        purchasesDF
            .groupBy(
                col("item"),
                window(col("time"), "1 day", "1 hour")
                    .as("time")) // struct column: has fields {start, end}
            .agg(sum("quantity").as("totalQuantity"))
            .select(
                col("time").getField("start").as("start"),
                col("time").getField("end").as("end"),
                col("item"),
                col("totalQuantity"))
            .orderBy(col("time"), col("totalQuantity").desc());

    bestSelling
        .writeStream()
        .format("console")
        .outputMode("complete") // output mode Complete
        .start()
        .awaitTermination();
  }

  public static void aggregateByProcessingTime() throws Exception {
    Dataset<Row> linesCharCountByWindowDF =
        spark
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 12346)
            .load()
            .select(
                col("value"),
                current_timestamp()
                    .as("processingTime")) // this is how you add processing time to a record
            .groupBy(window(col("processingTime"), "10 seconds").as("window"))
            .agg(
                sum(length(col("value")))
                    .as("charCount")) // counting characters every 10 seconds by processing time
            .select(
                col("window").getField("start").as("start"),
                col("window").getField("end").as("end"),
                col("charCount"));

    linesCharCountByWindowDF
        .writeStream()
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination();
  }

  public static void main(String[] args) throws Exception {
    // aggregatePurchasesBySlidingWindow();
    aggregatePurchasesByTumblingWindow();
  }
}
