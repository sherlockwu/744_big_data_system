import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
      
object PageRank {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.out.println("PageRank hdfs_path iterations [RDD_part_size]");
      return;
    }
    val filePath = args(0)
    val numIters = args(1).toInt
    var rddPartSize : Int = 10
    if (args.length == 3) {
      rddPartSize = args(2).toInt
    }
    System.out.println("Expected Partitions: " + rddPartSize)

    val conf = new SparkConf().setAppName("CS-744-Assignment1-PartC-1")
    val sc = new SparkContext(conf)

    // Load the edges as a graph
    val input = sc.textFile(filePath).filter(line => line(0) != '#')
    val graph = input.map(line => {val pair = line.split("\\W+");
                                   (pair(0).toInt, pair(1).toInt) })
                     .groupByKey().repartition(rddPartSize)
    
    System.out.println("graph is cached? " + graph.getStorageLevel.useMemory);
    var ranks = graph.mapValues(r => 1.0)

    System.out.println("Start iterating..");
    var i = 0
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
