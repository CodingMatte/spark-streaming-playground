package matteoincicco.javasparkplayground;

import matteoincicco.javasparkplayground.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

// import org.apache.spark.sql.catalyst.expressions.WindowSpec;
// import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.expressions.*;

import javax.xml.crypto.Data;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Main {

  private static String getFilePathFromResources(String resourcePath) {
    URL resourceUrl = Main.class.getClassLoader().getResource(resourcePath);
    if (resourceUrl == null) {
      throw new RuntimeException("Resource not found: " + resourcePath);
    }
    return Paths.get(resourceUrl.getPath()).toString();
  }

  public static void main(String[] args) {
    // initialize spark session
    SparkUtils sparkUtils = new SparkUtils();
    // reduce spark log level
    sparkUtils.setSparkLogLevel("ERROR");

    // load csv
    String filePathCsv = getFilePathFromResources("hotels_booking.csv");
    StructType schema =
        new StructType()
            .add("Name", DataTypes.StringType, true)
            .add("Amenities", DataTypes.StringType, false)
            .add("Stars", DataTypes.IntegerType, true)
            .add("City", DataTypes.StringType, true)
            .add("Country", DataTypes.StringType, true)
            .add("Photo", DataTypes.StringType, false)
            .add("Price", DataTypes.StringType, false)
            .add("WebSite", DataTypes.StringType, true);
    Dataset<Row> df_hotels_booking =
        sparkUtils
            .getSparkSession()
            .read()
            .format("csv")
            .option("header", "true")
            // .option("mode", "PERMISSIVE") // Options: PERMISSIVE, DROPMALFORMED, FAILFAST
            // .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schema)
            .load(filePathCsv);

    // Define schema
    StructType schemaAirbnb =
        new StructType()
            .add("Name", "string")
            .add("CategoryCode", "integer")
            .add("Amenities", "string")
            .add("Stars", "integer")
            .add("City", "string")
            .add("Country", "string")
            .add("Photo", "string")
            .add("Price", "integer");

    Dataset<Row> df_test_pinco_pallino =
        sparkUtils.loadJson(
            getFilePathFromResources("hotels_airbnb.json"), true, true, schemaAirbnb);

    df_test_pinco_pallino.show();

    //    // Define schema
    //    StructType schemaAirbnb =
    //        new StructType()
    //            .add("Name", "string")
    //            .add("CategoryCode", "integer")
    //            .add("Amenities", "string")
    //            .add("Stars", "integer")
    //            .add("City", "string")
    //            .add("Country", "string")
    //            .add("Photo", "string")
    //            .add("Price", "integer");
    //
    //    // load json
    //    Dataset<Row> df_hotels_airbnb =
    //        sparkUtils.loadJson(
    //            getFilePathFromResources("hotels_airbnb.json"), true, true, schemaAirbnb);
    //
    //    // load parquet
    //    Dataset<Row> df_hotels_subito =
    //        sparkUtils.loadParquet(getFilePathFromResources("hotels_subito"));
    //
    //    // load json lookup
    //    Dataset<Row> df_category_code =
    //        sparkUtils.loadJson(getFilePathFromResources("category_code.json"), true, true, null);
    //
    //    // Simple Union
    //    List<String> dfHotelsAirbnbColumns = Arrays.asList(df_hotels_airbnb.columns());
    //    List<String> dfHotelsBookingColumns = Arrays.asList(df_hotels_booking.columns());
    //
    //    List<String> diffNotContainsBooking =
    //        dfHotelsAirbnbColumns.stream()
    //            .filter(c -> !dfHotelsBookingColumns.contains(c))
    //            .collect(Collectors.toList());
    //    List<String> diffNotContainsAirBnb =
    //        dfHotelsBookingColumns.stream()
    //            .filter(c -> !dfHotelsAirbnbColumns.contains(c))
    //            .collect(Collectors.toList());
    //
    //    Dataset<Row> df_hotels_booking_c = df_hotels_booking;
    //    for (String column : diffNotContainsBooking) {
    //      df_hotels_booking_c =
    //          df_hotels_booking_c.withColumn(column, lit(null).cast(DataTypes.StringType));
    //    }
    //    Dataset<Row> df_hotels_airbnb_c = df_hotels_airbnb;
    //    for (String column : diffNotContainsAirBnb) {
    //      df_hotels_airbnb_c =
    //          df_hotels_airbnb_c.withColumn(column, lit(null).cast(DataTypes.StringType));
    //    }
    //
    //    Dataset<Row> df_hotels_booking_f = df_hotels_booking_c.withColumn("source",
    // lit("booking"));
    //    Dataset<Row> df_hotels_airbnb_f = df_hotels_airbnb_c.withColumn("source", lit("airbnb"));
    //    Dataset<Row> df_hotels_subito_f =
    //        df_hotels_subito.select(
    //            col("*"), lit(1).alias("CategoryCode"), lit("subito").alias("source"));
    //    String[] columnsList = df_hotels_booking_f.columns();
    //
    //    Dataset<Row> dfUnion =
    //        (df_hotels_booking_f
    //                .selectExpr(columnsList)
    //                .union(df_hotels_airbnb_f.selectExpr(columnsList))
    //                .union(df_hotels_subito_f.selectExpr(columnsList)))
    //            .as("A")
    //            .join(
    //                broadcast(df_category_code.alias("B")),
    //                col("A.CategoryCode").equalTo(col("B.CategoryCode")),
    //                "left");
    //    dfUnion.show();
    //
    //    Dataset<Row> dfAggregated =
    //        dfUnion.groupBy("CategoryName").agg(sum("Price").cast("int"),
    // max("Price").cast("int"));
    //    dfAggregated.show();
    //
    //    WindowSpec windowSpec = Window.partitionBy("source").orderBy(col("Price").desc());
    //
    //    Dataset<Row> dfWindow1 =
    //        dfUnion
    //            .select("Name", "source", "Price")
    //            .withColumn("row_number", row_number().over(windowSpec))
    //            .withColumn("rank", rank().over(windowSpec))
    //            .withColumn("dense_rank", dense_rank().over(windowSpec))
    //            .orderBy(col("source"), col("row_number"))
    //            .filter(col("dense_rank").leq(5));
    //
    //    dfWindow1.show(40);
    //
    //    StructType schemaJson =
    //        new StructType()
    //            .add("name", DataTypes.StringType, true)
    //            .add(
    //                "address",
    //                new StructType()
    //                    .add("city", DataTypes.StringType, true)
    //                    .add("state", DataTypes.StringType, true),
    //                true)
    //            .add(
    //                "contacts",
    //                DataTypes.createArrayType(
    //                    new StructType()
    //                        .add("type", DataTypes.StringType, true)
    //                        .add("value", DataTypes.StringType, true)),
    //                true);
    //    // flatten
    //    Dataset<Row> dfToFlatten2 =
    //        sparkUtils
    //            .getSparkSession()
    //            .read()
    //            // .schema(schemaJson)
    //            .option("multiline", true)
    //            .option("inferschema", true)
    //            .json("src/main/resources/json_to_flat.json");
    //    dfToFlatten2.show(false);
    //    Dataset<Row> dfToFlatten =
    //        sparkUtils.loadJson(getFilePathFromResources("json_to_flat.json"), true, true,
    // schemaJson);
    //    dfToFlatten.show(false);
    //
    //    Dataset<Row> dfFlatStaging =
    //        dfToFlatten.select(
    //            col("name"),
    //            col("address.city").as("city"),
    //            col("address.state").as("state"),
    //            explode(col("contacts")).as("contacts"));
    //    dfFlatStaging.show(false);
    //
    //    Dataset<Row> dfFlatComplete =
    //        dfFlatStaging.select(
    //            col("name"),
    //            col("city"),
    //            col("state"),
    //            col("contacts.type").as("contacts_type"),
    //            col("contacts.value").as("contacts_value"));
    //    dfFlatComplete.show(false);
    //
    //    sparkUtils.publishGenreStats(dfFlatComplete);

    // stop spark session
    sparkUtils.stopSparkSession();
  }
}
