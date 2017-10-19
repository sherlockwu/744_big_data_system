import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object PartAQuestion2 {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: PartAQuestion2 inputDir")
      System.exit(1)
    }

    val filePath = args(0)

    val spark = SparkSession
      .builder
      .appName("PartAQuestion2")
      .getOrCreate()

    import spark.implicits._

    val schema = StructType(
      Array(StructField("userA", StringType),
        StructField("userB", StringType),
        StructField("timestamp", TimestampType),
        StructField("interaction", StringType)))

    val fileStreamDf = spark.readStream
      .option("header", "true")
      .schema(schema)
      .csv(filePath)

    // Group the data by window and word and compute the count of each group
    val selectedUser = fileStreamDf.select("userB").where("interaction = 'MT'")

    val query = selectedUser.writeStream.format("csv")
      .option("path", "/assignment2/PartA/Q2_result")
      .option("checkpointLocation", "/assignment2/PartA/checkpoint_dir")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"));

    query.start().awaitTermination()
  }
}
