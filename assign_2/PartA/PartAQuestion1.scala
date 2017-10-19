import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object PartAQuestion1 {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: PartAQuestion1 inputDir")
      System.exit(1)
    }

    val filePath = args(0)

    val spark = SparkSession
      .builder
      .appName("PartAQuestion1")
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
    val windowedCounts = fileStreamDf.groupBy(
      window($"timestamp", "60 minutes", "30 minutes"), $"interaction"
    ).count().orderBy("window")

    val query = windowedCounts.writeStream.format("console")
      .option("numRows", 563500)
      .option("truncate", false)
      .outputMode("complete")

    query.start().awaitTermination()
  }
}
