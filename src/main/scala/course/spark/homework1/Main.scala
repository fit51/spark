package course.spark.homework1

import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {
  // Our Graph model
  type VertexId = Long
  case class Edge(srcId: VertexId, destId: VertexId, distance: Double)

  def main(args: Array[String]): Unit = {
    // Initialise spark session or return existing one. SparkSession initialises SparkContext,
    // which represents the connection to a Spark cluster.
    val spark: SparkSession = SparkSession.builder()
      .appName("Homework1")
      .master("local[*]")
      .config("spark.executor.memory", "1g")
      .getOrCreate()

    val numVertices = 1000
    val numGraphPartitions = 10

    // Generate random graph with random distances between vertices
    val graph = GraphGenerators
      .logNormalGraph(spark.sparkContext, numVertices, numGraphPartitions)
      .triplets
      .map(t => Edge(t.srcId, t.dstId, Math.random()))

    val edgeNums: RDD[(VertexId, Int)] = computeOutgoingEdgeNums(graph)

    val sampleSizePerPartition = 20
    val numTargetPartitions = 8

    val sortedEdgeNums = RDDSorter
      .sampleSort(sampleSizePerPartition, numTargetPartitions)(edgeNums)

    sortedEdgeNums.mapPartitionsWithIndex { case (partitionId, it) =>
      println("Partition " + partitionId + " values: " + it.mkString(";"))
      Iterator(1)
    }.count() // count is an action used to start execution

    // Uncomment the line below to see UI metrics and DAG before this Spark application exits
    // (the url is specified in `` message in the output):
    // Thread.sleep(10000000)

    spark.stop()
  }

  /**
    * Compute number of outgoing edges per vertex in graph
    *
    * @param graph input graph
    * @return RDD of (VertexId, number of outgoing edges) pairs
    */
  def computeOutgoingEdgeNums(graph: RDD[Edge]): RDD[(VertexId, Int)] =
    graph
      .map(e => (e.srcId, 1))
      // reduceByKey function takes `func: (V, V) => V` lambda and applies it to all values of each key.
      // This function is defined for RDD[T <: Tuple2] only and uses first element of a tuple as a key
      .reduceByKey(_ + _)
}
