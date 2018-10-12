# Spark course homework
## Homework 1. MapReduce sort
Implement Sample Sort without using built-in RDD functions `sortByKey`, `sample` and `repartitionAndSortWithinPartitions` in file RDDSorter.scala. List of all available Spark transformations - https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations

After completion of this assignment run tests to check sampleSort works correctly by running:
```
sbt "test:testOnly *RDDSorterTest"
```
It is advisable to add your own tests for each function and tests for corner cases.

Also, Main class provides usage example, which orders graph vertices by the number of outgoing edges. Remove `println` to play with bigger graphs. To run it, type:
```
sbt "runMain course.spark.homework1.Main"
```
