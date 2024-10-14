package structuredstreaming;

import java.util.concurrent.TimeoutException;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import static common.Schemas.carsSchema;
import common.Car;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import utils.SparkUtils;
import org.apache.spark.sql.Encoders;
import static org.apache.spark.sql.functions.*;
import scala.Tuple2;

public class StreamingDatasets {

  // initialize spark session from spark utils
  private static final SparkUtils sparkUtils = new SparkUtils("Streaming Datasets", "local[*]");
  private static final SparkSession spark = sparkUtils.getSparkSession();

  public static Dataset<Car> readCars() {
    // Encoder for Car class
    return spark
        .readStream()
        .format("socket")
        .option("host", "localhost")
        .option("port", 12345)
        .load() // DataFrame with single string column "value"
        .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
        .selectExpr("car.*") // DataFrame with multiple columns
        .as(Encoders.bean(Car.class)); // Use Encoders for transformation from DF to DS
  }

  public static void showCarNames() throws Exception {
    Dataset<Car> carsDS = readCars();

    // collection transformations maintain type info
    Dataset<String> carNamesAlt =
        carsDS.map((MapFunction<Car, String>) Car::getName, Encoders.STRING());

    carNamesAlt.writeStream().format("console").outputMode("append").start().awaitTermination();
  }

  /**
   * Exercises
   *
   * <p>1 - Count how many POWERFUL cars we have in the DS (HP > 140) 2 - Average HP for the entire
   * dataset 3 - Count the cars by origin
   */
  public static void ex1() throws StreamingQueryException, TimeoutException {
    Dataset<Car> carsDS = readCars();
    carsDS
        .filter((Car car) -> car.getHorsepower() != null && car.getHorsepower() > 140)
        .writeStream()
        .format("console")
        .outputMode("append")
        .start()
        .awaitTermination();
  }

  public static void ex2() throws StreamingQueryException, TimeoutException {
    Dataset<Car> carsDS = readCars();
    carsDS
        .select(avg(col("Horsepower")))
        .writeStream()
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination();
  }

  public static void ex3() throws StreamingQueryException, TimeoutException {
    Dataset<Car> carsDS = readCars();

    // Option 1: Group by column "Origin"
    Dataset<Row> carCountByOrigin = carsDS.groupBy(col("Origin")).count();

    // Option 2: Group by key (Origin) using Dataset API
    Dataset<Tuple2<String, Object>> carCountByOriginAlt =
        carsDS
            .groupByKey((MapFunction<Car, String>) car -> car.getOrigin(), Encoders.STRING())
            .count();

    carCountByOriginAlt
        .writeStream()
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination();
  }

  public static void main(String[] args) throws Exception {
    sparkUtils.setSparkLogLevel("ERROR");
    // showCarNames();
    // ex1();
    // ex2();
    ex3();
  }
}
