package utils;

import java.util.Map;
import java.util.HashMap;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {
  // I'm not using static because is better if we have in plan to expand the functionality for
  // better testability and flexibility
  private SparkSession sparkSession;

  // Constructor with optional additionalConfigs
  public SparkUtils(String appName, String master) {
    this(appName, master, new HashMap<>()); // Call the other constructor with an empty map
  }

  // Main constructor
  public SparkUtils(String appName, String master, Map<String, String> additionalConfigs) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName(appName)
            .master(master)
            .config(
                "spark.driver.extraJavaOptions",
                "--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED "
                    + "--add-opens java.base/java.nio=ALL-UNNAMED --add-exports java.base/java.nio=ALL-UNNAMED "
                    + "--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED")
            .config(
                "spark.executor.extraJavaOptions",
                "--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED "
                    + "--add-opens java.base/java.nio=ALL-UNNAMED --add-exports java.base/java.nio=ALL-UNNAMED "
                    + "--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED");

    // Apply the configurations from the map
    for (Map.Entry<String, String> entry : additionalConfigs.entrySet()) {
      builder.config(entry.getKey(), entry.getValue());
    }

    this.sparkSession = builder.getOrCreate();
  }

  // Getter
  public SparkSession getSparkSession() {
    return sparkSession;
  }

  // Setter
  public void setSparkLogLevel(String level) {
    this.sparkSession.sparkContext().setLogLevel(level);
  }
}
