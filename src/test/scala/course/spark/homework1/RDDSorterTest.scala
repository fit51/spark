package course.spark.homework1

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.FunSuite

import scala.util.Random

class RDDSorterTest extends FunSuite with SharedSparkContext with RDDComparisons {
  test("test RDDSorter") {
    val numPartitions = 11
    val input = sc.parallelize(Random.shuffle(Range(0, 100).map(i => (100 - i, i))), numPartitions)
    input.mapPartitionsWithIndex { case (index, it) =>
      println("Partition " + index + " values: " + it.map(_._2).mkString(";"))
      Iterator(1)
    }.count()

    val sampleSizePerPartition = 3
    val numTargetPartitions = 4

    val sortedRDD = RDDSorter.sampleSort(sampleSizePerPartition, numTargetPartitions)(input)
    sortedRDD.mapPartitionsWithIndex { case (index, it) =>
      println("Partition " + index + " values: " + it.map(_._2).mkString(";"))
      Iterator(1)
    }.count()

    val expectedRDD = sc.parallelize(Range(100, 0, -1).map(i => (i, 100 - i)), numTargetPartitions)
    expectedRDD.mapPartitionsWithIndex { case (index, it) =>
      println("Partition " + index + " values: " + it.map(_._2).mkString(";"))
      Iterator(1)
    }.count()

    assertRDDEqualsWithOrder(expectedRDD, sortedRDD)
  }
}
