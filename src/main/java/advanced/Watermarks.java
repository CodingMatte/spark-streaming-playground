package advanced;

import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import scala.Tuple2;
import utils.SparkUtils;

public class Watermarks {
  // initialize spark session from spark utils
  private static final SparkUtils sparkUtils =
      new SparkUtils("Late Data with Watermarks", "local[*]");
  private static final SparkSession spark = sparkUtils.getSparkSession();

  // --add-opens java.base/java.nio=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED

  public static void debugQuery(StreamingQuery query) {
    new Thread(
            () -> {
              for (int i = 1; i <= 100; i++) {
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
                String queryEventTime =
                    (query.lastProgress() == null)
                        ? "[]"
                        : query.lastProgress().eventTime().toString();
                System.out.println(i + ": " + queryEventTime);
              }
            })
        .start();
  }

  public static void testWatermark() throws Exception {
    Dataset<Row> dataDF =
        spark
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 12345)
            .load()
            .as(Encoders.STRING())
            .map(
                (MapFunction<String, Tuple2<Timestamp, String>>)
                    line -> {
                      String[] tokens = line.split(",");
                      Timestamp timestamp = new Timestamp(Long.parseLong(tokens[0]));
                      String data = tokens[1];
                      return new scala.Tuple2<>(timestamp, data);
                    },
                Encoders.tuple(Encoders.TIMESTAMP(), Encoders.STRING()))
            .toDF("created", "color");

    Dataset<Row> watermarkedDF =
        dataDF
            .withWatermark("created", "2 seconds")
            .groupBy(window(col("created"), "2 seconds"), col("color"))
            .count()
            .selectExpr("window.*", "color", "count");

    /*
     A 2-second watermark means
     - a window will only be considered until the watermark surpasses the window end
     - an element/a row/a record will be considered if AFTER the watermark
    */

    StreamingQuery query =
        watermarkedDF
            .writeStream()
            .format("console")
            .outputMode("append")
            .trigger(Trigger.ProcessingTime("2 seconds"))
            .start();

    debugQuery(query);
    query.awaitTermination();
  }

  public static void main(String[] args) throws Exception {
    testWatermark();
  }
}

// sending data "manually" through socket to be as deterministic as possible
class DataSender {
  private static ServerSocket serverSocket;
  private static Socket socket;
  private static PrintStream printer;

  static {
    try {
      serverSocket = new ServerSocket(12345);
      socket = serverSocket.accept(); // blocking call
      printer = new PrintStream(socket.getOutputStream());
      System.out.println("socket accepted");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void example1() throws InterruptedException {
    Thread.sleep(7000);
    printer.println("7000,blue");
    Thread.sleep(1000);
    printer.println("8000,green");
    Thread.sleep(4000);
    printer.println("14000,blue");
    Thread.sleep(1000);
    printer.println("9000,red"); // discarded: older than the watermark
    Thread.sleep(3000);
    printer.println("15000,red");
    printer.println("8000,blue"); // discarded: older than the watermark
    Thread.sleep(1000);
    printer.println("13000,green");
    Thread.sleep(500);
    printer.println("21000,green");
    Thread.sleep(3000);
    printer.println("4000,purple"); // expect to be dropped - it's older than the watermark
    Thread.sleep(2000);
    printer.println("17000,green");
  }

  public static void example2() throws InterruptedException {
    printer.println("5000,red");
    printer.println("5000,green");
    printer.println("4000,blue");

    Thread.sleep(7000);
    printer.println("1000,yellow");
    printer.println("2000,cyan");
    printer.println("3000,magenta");
    printer.println("5000,black");

    Thread.sleep(3000);
    printer.println("10000,pink");
  }

  public static void example3() throws InterruptedException {
    Thread.sleep(2000);
    printer.println("9000,blue");
    Thread.sleep(3000);
    printer.println("2000,green");
    printer.println("1000,blue");
    printer.println("8000,red");
    Thread.sleep(2000);
    printer.println("5000,red"); // discarded
    printer.println("18000,blue");
    Thread.sleep(1000);
    printer.println("2000,green"); // discarded
    Thread.sleep(2000);
    printer.println("30000,purple");
    printer.println("10000,green");
  }

  public static void main(String[] args) throws InterruptedException {
    example3();
  }
}
