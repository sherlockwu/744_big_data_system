package cs744.a3

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object PartBApplication1Question1 {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: PartBApplication1Question1 inputDir numIters")
      System.exit(1)
    }

    val filePath = args(0)
    val numIters: Int = args(1).toInt
    val outPath: String = "/assignment3/PartB/Application1/"

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    val graph = GraphLoader.edgeListFile(sc, filePath)
    val outDegGraph = graph.outerJoinVertices(graph.outDegrees){(_, _, degOpt) => degOpt.getOrElse(0)}.persist()

    var ranks = graph.vertices.mapValues(r => 1.0)  // initialization

    for (i <- 1 to numIters) {
      val outputGraph = outDegGraph.outerJoinVertices(ranks){(_, deg, rank) => rank.getOrElse(0.15) / deg}
      val contri = outputGraph.aggregateMessages[(Double)](
        triplet => { triplet.sendToDst(triplet.srcAttr) },  // map
        _ + _   // reduce
      )
      ranks = contri.mapValues(r => 0.15 + 0.85 * r)
    }

    val graphWithFinalRanks = graph.outerJoinVertices(ranks){(_, _, rank) => (rank.getOrElse(0.15))}
    graphWithFinalRanks.vertices.collect();
    System.out.println("Save result to " + outPath)
    graphWithFinalRanks.vertices.saveAsTextFile(outPath)
  }
}
