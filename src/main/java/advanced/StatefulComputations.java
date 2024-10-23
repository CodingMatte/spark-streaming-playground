package advanced;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.StreamingQueryException;
import utils.SparkUtils;

public class StatefulComputations {
  // initialize spark session from spark utils
  private static final SparkUtils sparkUtils = new SparkUtils("Event Time Windows", "local[*]");
  private static final SparkSession spark = sparkUtils.getSparkSession();

  // --add-opens java.base/java.nio=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED

  public static class SocialPostRecord implements Serializable {
    private String postType;
    private int count;
    private int storageUsed;

    // Default constructor
    public SocialPostRecord() {
      this.postType = "";
      this.count = 0;
      this.storageUsed = 0;
    }

    // Constructor
    public SocialPostRecord(String postType, int count, int storageUsed) {
      this.postType = postType;
      this.count = count;
      this.storageUsed = storageUsed;
    }

    // Getters and Setters
    public String getPostType() {
      return postType;
    }

    public void setPostType(String postType) {
      this.postType = postType;
    }

    public int getCount() {
      return count;
    }

    public void setCount(int count) {
      this.count = count;
    }

    public int getStorageUsed() {
      return storageUsed;
    }

    public void setStorageUsed(int storageUsed) {
      this.storageUsed = storageUsed;
    }
  }

  public static class SocialPostBulk {
    private String postType;
    private int count;
    private int totalStorageUsed;

    // Constructor
    public SocialPostBulk(String postType, int count, int totalStorageUsed) {
      this.postType = postType;
      this.count = count;
      this.totalStorageUsed = totalStorageUsed;
    }

    // Getters and Setters
    public String getPostType() {
      return postType;
    }

    public void setPostType(String postType) {
      this.postType = postType;
    }

    public int getCount() {
      return count;
    }

    public void setCount(int count) {
      this.count = count;
    }

    public int getTotalStorageUsed() {
      return totalStorageUsed;
    }

    public void setTotalStorageUsed(int totalStorageUsed) {
      this.totalStorageUsed = totalStorageUsed;
    }
  }

  public static class AveragePostStorage {
    private String postType;
    private double averageStorage;

    // Constructor
    public AveragePostStorage(String postType, double averageStorage) {
      this.postType = postType;
      this.averageStorage = averageStorage;
    }

    // Getters and Setters
    public String getPostType() {
      return postType;
    }

    public void setPostType(String postType) {
      this.postType = postType;
    }

    public double getAverageStorage() {
      return averageStorage;
    }

    public void setAverageStorage(double averageStorage) {
      this.averageStorage = averageStorage;
    }
  }

  public static Dataset<SocialPostRecord> readSocialUpdates() {
    return spark
        .readStream()
        .format("socket")
        .option("host", "localhost")
        .option("port", 12345)
        .load()
        .as(Encoders.STRING())
        .map(
            (MapFunction<String, SocialPostRecord>)
                line -> {
                  String[] tokens = line.split(",");
                  return new SocialPostRecord(
                      tokens[0], Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
                },
            Encoders.bean(SocialPostRecord.class));
  }

  public static AveragePostStorage updateAverageStorage(
      String postType, Iterator<SocialPostRecord> group, GroupState<SocialPostBulk> state) {

    SocialPostBulk previousBulk = state.exists() ? state.get() : new SocialPostBulk(postType, 0, 0);

    // Aggregate the data
    int totalCount = 0;
    int totalStorage = 0;
    while (group.hasNext()) {
      SocialPostRecord record = group.next();
      totalCount += record.getCount();
      totalStorage += record.getStorageUsed();
    }

    // Update the state
    SocialPostBulk newBulk =
        new SocialPostBulk(
            postType,
            previousBulk.getCount() + totalCount,
            previousBulk.getTotalStorageUsed() + totalStorage);
    state.update(newBulk);

    // Return the result
    double averageStorage = newBulk.getTotalStorageUsed() * 1.0 / newBulk.getCount();
    return new AveragePostStorage(postType, averageStorage);
  }

  public static void getAveragePostStorage() throws StreamingQueryException, TimeoutException {
    Dataset<SocialPostRecord> socialStream = readSocialUpdates();

    // Compute average by post type
    Dataset<AveragePostStorage> averageByPostType =
        socialStream
            .groupByKey(
                (MapFunction<SocialPostRecord, String>) SocialPostRecord::getPostType,
                Encoders.STRING())
            .mapGroupsWithState(
                (MapGroupsWithStateFunction<
                        String, SocialPostRecord, SocialPostBulk, AveragePostStorage>)
                    StatefulComputations::updateAverageStorage,
                Encoders.bean(SocialPostBulk.class),
                Encoders.bean(AveragePostStorage.class),
                GroupStateTimeout.NoTimeout());

    averageByPostType
        .writeStream()
        .outputMode("update")
        .foreachBatch(
            (VoidFunction2<Dataset<AveragePostStorage>, Long>) (batch, batchId) -> batch.show())
        .start()
        .awaitTermination();
  }

  public static void main(String[] args) throws StreamingQueryException, TimeoutException {
    getAveragePostStorage();
  }
}

/*
-- batch 1
text,3,3000
text,4,5000
video,1,500000
audio,3,60000
-- batch 2
text,1,2500

average for text = 10500 / 8
 */
