package cs744.a3

import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

object PartBApplication2Question4 {
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
    
    // create a new graph with edge data the intersection of src.Set and dst.Set
    val graphE = graph.mapTriplets (
      triplet => triplet.srcAttr.asInstanceOf[Set[String]]
                        .intersect(triplet.dstAttr.asInstanceOf[Set[String]]).toArray
    )

    // extra the word list from all edges
    val words : Array[String] = graphE.edges.map(edge => edge.attr).reduce(_ ++ _)

    // word count
    val wordRDD = sc.parallelize(words).map(v => (v, 1)).reduceByKey(_ + _)

    val res = wordRDD.map(_.swap).sortByKey(false).take(1)(0)
    
    println("most popular word: " + res._2 + ", count=" + res._1)
  }
}
