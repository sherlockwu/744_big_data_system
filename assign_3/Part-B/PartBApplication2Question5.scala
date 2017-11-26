package cs744.a3

import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

object PartBApplication2Question5 {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // load edges and vertices and composite the graph
    val edges = GraphLoader.edgeListFile(sc, "/assignment3/PartB/Application2/data/edges.txt")
    val vertices = sc.textFile("/assignment3/PartB/Application2/data/vertices.txt")
                     .map(line => line.split(",").toSet)
                     .zipWithIndex().map(_.swap)

    val graph = Graph(vertices, edges.edges)

    val subGraphSizes = graph.connectedComponents.vertices.map(v => (v._2, 1)).reduceByKey(_ + _)
        
    val res = subGraphSizes.map(_.swap).sortByKey(false).take(1)(0)

    println("largest subgraph size: " + res._1)
  }
}
