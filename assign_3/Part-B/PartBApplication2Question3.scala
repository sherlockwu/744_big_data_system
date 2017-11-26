package cs744.a3

import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

object PartBApplication2Question3 {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // load edges and vertices and composite the graph
    val edges = GraphLoader.edgeListFile(sc, "/assignment3/PartB/Application2/data/edges.txt")
    val vertices = sc.textFile("/assignment3/PartB/Application2/data/vertices.txt").map(line => line.split(","))
                     .zipWithIndex().map(_.swap)

    val graph = Graph(vertices, edges.edges)

    // aggregate messages
    val neighborSizes = graph.aggregateMessages[(Int, Double)] (
      triplet => {triplet.sendToDst(1, triplet.srcAttr.length)},
      (a, b) => (a._1 + b._1, a._2 + b._2)
    )
    // compute the average
    val averageSize = neighborSizes.mapValues( 
      (id, value) => value match { case (count, totalNum) => totalNum / count}
    )
    println("average number of words in every neighbor of a vertex: ")
    averageSize.collect.foreach(println)
  }
}
