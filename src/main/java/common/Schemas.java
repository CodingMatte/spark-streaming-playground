package common;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Schemas {

  public static final StructType carsSchema =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("Name", DataTypes.StringType, true),
            DataTypes.createStructField("Miles_per_Gallon", DataTypes.DoubleType, true),
            DataTypes.createStructField("Cylinders", DataTypes.LongType, true),
            DataTypes.createStructField("Displacement", DataTypes.DoubleType, true),
            DataTypes.createStructField("Horsepower", DataTypes.LongType, true),
            DataTypes.createStructField("Weight_in_lbs", DataTypes.LongType, true),
            DataTypes.createStructField("Acceleration", DataTypes.DoubleType, true),
            DataTypes.createStructField("Year", DataTypes.StringType, true),
            DataTypes.createStructField("Origin", DataTypes.StringType, true)
          });

  public static final StructType stocksSchema =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("company", DataTypes.StringType, true),
            DataTypes.createStructField("date", DataTypes.DateType, true),
            DataTypes.createStructField("value", DataTypes.DoubleType, true)
          });
}
