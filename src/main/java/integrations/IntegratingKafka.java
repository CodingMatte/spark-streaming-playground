package integrations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.SparkUtils;

import static common.Schemas.carsSchema;
import static org.apache.spark.sql.functions.*;

public class IntegratingKafka {

  // initialize spark session from spark utils
  private static final SparkUtils sparkUtils = new SparkUtils("Streaming Datasets", "local[*]");
  private static final SparkSession spark = sparkUtils.getSparkSession();

  public static void readFromKafka() throws Exception {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    Dataset<Row> kafkaDF =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "rockthejvm")
            .load();

    kafkaDF
        .select(col("topic"), expr("cast(value as string) as actualValue"))
        .writeStream()
        .format("console")
        .outputMode("append")
        .start()
        .awaitTermination();
  }

  public static void writeToKafka() throws Exception {
    Dataset<Row> carsDF =
        spark.readStream().schema(carsSchema).json("src/main/resources/data/cars");

    Dataset<Row> carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value");

    carsKafkaDF
        .writeStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "rockthejvm")
        .option(
            "checkpointLocation",
            "checkpoints") // without checkpoints the writing to Kafka will fail
        .start()
        .awaitTermination();
  }

  /**
   * Exercise: write the whole cars data structures to Kafka as JSON. Use struct columns an the
   * to_json function.
   */
  public static void writeCarsToKafka() throws Exception {
    Dataset<Row> carsDF =
        spark.readStream().schema(carsSchema).json("src/main/resources/data/cars");

    Dataset<Row> carsJsonKafkaDF =
        carsDF.select(
            col("Name").as("key"),
            to_json(struct(col("Name"), col("Horsepower"), col("Origin")))
                .cast("String")
                .as("value"));

    carsJsonKafkaDF
        .writeStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "rockthejvm")
        .option("checkpointLocation", "checkpoints")
        .start()
        .awaitTermination();
  }

  public static void main(String[] args) throws Exception {
    // Remember to delete checkpoints folder before running the code
    // readFromKafka();
    // writeToKafka();
    writeCarsToKafka();
  }
}
