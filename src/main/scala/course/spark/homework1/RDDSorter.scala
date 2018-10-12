package course.spark.homework1

import org.apache.spark.rdd.RDD
import Extensions._

import scala.math.Ordering
import scala.reflect.ClassTag

object RDDSorter {
  /**
    * Sort rdd of (key, value) pairs by value using Sample Sort algorithm
    * (http://parallelcomp.uw.hu/ch09lev1sec5.html)
    *
    * @param rdd input
    * @param sampleSizePerPartition number of elements to sample on each partition. Total sample
    *                               size will be sampleSizePerPartition * rdd.getNumPartitions
    * @param numTargetPartitions number of result rdd partitions
    * @return totally ordered RDD, where if i < j => partition i values < partition j values
    */
  def sampleSort[K, V : ClassTag](sampleSizePerPartition: Int, numTargetPartitions: Int)(rdd: RDD[(K, V)])
      (implicit ord: Ordering[V]): RDD[(K, V)] = {
    val sample = sampleRDD(rdd.map(_._2), sampleSizePerPartition) // sampling values only
    val bounds = computePartitionBounds(sample, numTargetPartitions)

    rdd
      .transform(repartitionWithBounds(bounds))
      .transform(sortWithinPartitions())
  }

  /**
    * Sample input rdd randomly by sampling sampleSizePerPartition elements on each partition
    *
    * @param rdd input
    * @param sampleSizePerPartition number of elements to sample on each partition
    * @return collected on driver sample from all partitions
    */
  def sampleRDD[T : ClassTag](rdd: RDD[T], sampleSizePerPartition: Int): Seq[T] = ???

  /**
    * Compute partition bounds approximately using sample of input rdd
    *
    * @param sample a sample used to estimate partition bounds
    * @param numTargetPartitions number of target partitions
    * @return Seq(p1, p2, .., pN), where values < p1 go to partition 0, values in [p1, p2) to partition 1 and so on
    */
  def computePartitionBounds[T](sample: Seq[T], numTargetPartitions: Int)(implicit ord: Ordering[T]): Seq[T] = ???

  /**
    * Repartition input rdd using bounds provided
    *
    * @param rdd input
    * @param bounds Seq(p1, p2, .., pN), where values < p1 go to partition 0, values in [p1, p2) to partition 1
    *               and so on
    * @return repartitioned rdd
    */
  def repartitionWithBounds[K, V](bounds: Seq[V])(rdd: RDD[(K, V)])(implicit ord: Ordering[V]): RDD[(K, V)] = ???

  /**
    * Sort values within each rdd partition
    *
    * @param rdd input
    * @return result rdd
    */
  def sortWithinPartitions[K, V]()(rdd: RDD[(K, V)])(implicit ord: Ordering[V]): RDD[(K, V)] = ???
}
