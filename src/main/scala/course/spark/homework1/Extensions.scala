package course.spark.homework1

import org.apache.spark.rdd.RDD

object Extensions {
  implicit class RDDExtended[A](rdd: RDD[A]) {
    def transform[B](f: RDD[A] => RDD[B]): RDD[B] = f(rdd)
  }
}
