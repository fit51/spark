package course.spark.homework1

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.FunSuite

class RDDSorterTest extends FunSuite with SharedSparkContext with RDDComparisons {
  test("test RDDSorter") {
    val numPartitions = 11
    val input = sc.parallelize(Range(0, 100).map(i => (100 - i, i)), numPartitions)

    val sampleSizePerPartition = 3
    val numTargetPartitions = 4

    val sortedRDD = RDDSorter.sampleSort(sampleSizePerPartition, numTargetPartitions)(input)
    val expectedRDD = sc.parallelize(Range(100, 0, -1).map(i => (i, 100 - i)), numTargetPartitions)

    assertRDDEqualsWithOrder(expectedRDD, sortedRDD)
  }
}
