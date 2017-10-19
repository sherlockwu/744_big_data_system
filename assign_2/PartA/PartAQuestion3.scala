import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object PartAQuestion3 {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: PartAQuestion3 inputDir")
      System.exit(1)
    }

    val filePath = args(0)
    val staticListPath = "/assignment2/PartA/static_list.txt"

    val spark = SparkSession
      .builder
      .appName("PartAQuestion3")
      .getOrCreate()

    import spark.implicits._

    val schema = StructType(
      Array(StructField("userA", StringType),
        StructField("userB", StringType),
        StructField("timestamp", TimestampType),
        StructField("interaction", StringType)))

    val whiteList = spark.read
      .option("header", "true")
      .schema(StructType(Array(StructField("userA", StringType))))
      .csv(staticListPath)

    whiteList.show() 

    val fileStreamDf = spark.readStream
      .schema(schema)
      .csv(filePath)

    val filteredCounts = fileStreamDf.join(whiteList, "userA").groupBy("userA").count()

    val query = filteredCounts.writeStream.format("console")
      .option("truncate", false)
      .option("numRows", 563500)
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("5 seconds"));

    query.start().awaitTermination()
  }
}
