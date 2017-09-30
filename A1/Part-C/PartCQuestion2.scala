import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD 

class SizePartitioner(data: RDD[(Int, Iterable[Int])], numParts: Int) extends Partitioner {
  var data_ = data
  var numParts_ = numParts
  override def numPartitions(): Int = numParts_
  override def getPartition(key: Any): Int = data_.lookup(key.asInstanceOf[Int]).size % numParts_
}

object PageRank {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.out.println("PageRank hdfs_path iterations [RDD_part_size]");
      return;
    }
    val filePath = args(0)
    val numIters = args(1).toInt
    var rddPartSize : Int = 2
    var partMode : String = "hash"
    if (2 < args.length) {
      rddPartSize = args(2).toInt
    }
    if (3 < args.length) {
      partMode = args(3)
    }
    System.out.println("Expected Partitions: " + rddPartSize)
    System.out.println("Partition mode: " + partMode)

    val conf = new SparkConf().setAppName("CS-744-Assignment1-PartC-2")
    val sc = new SparkContext(conf)
    

    // Load the edges as a graph
    val input = sc.textFile(filePath).filter(line => line(0) != '#')
    val graphRaw = input.map(line => {val pair = line.split("\\W+");
                                   (pair(0).toInt, pair(1).toInt) })
                     .groupByKey()
    // create partitioner
    val partitioner : Partitioner = new RangePartitioner(rddPartSize, graphRaw)
    // val partitioner = new HashPartitioner(rddPartSize)
    // val partitioner = new SizePartitioner(graphRaw, rddPartSize)

    val graph = graphRaw.partitionBy(partitioner).repartition(rddPartSize)

    System.out.println("graph is cached? " + graph.getStorageLevel.useMemory);
    var ranks = graph.mapValues(r => 1.0)

    System.out.println("Start iterating..");
    for (i <- 1 to numIters) {
      val delta = graph.join(ranks).values.flatMap(
        {
          case (neighbors, r) =>
            val outdeg = neighbors.size
            neighbors.map( v => (v, r / outdeg))
        })
      ranks = delta.reduceByKey( (a, b) => a + b ).mapValues(r => 0.15 + 0.85 * r)
    }
    System.out.println("Print first 10 elements:");
    ranks.take(10).foreach(println);
    System.out.println("Finished.");
  }
}
