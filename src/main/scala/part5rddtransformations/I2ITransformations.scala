package part5rddtransformations

import generator.DataGenerator
import org.apache.spark.sql.SparkSession
import scala.collection.mutable

object I2ITransformations {

  val spark = SparkSession.builder()
    .appName("I2I Transformation")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
    Science project
    each metric has identifier, value

    Return the smallest ("best") 10 metrics (identifiers + values)
   */

  val LIMIT = 10

  def readMetrics() = sc.textFile("src/main/resources/generated/metrics/metrics10m.txt")
    .map { line =>
      val tokens = line.split(" ")
      val name = tokens(0)
      val value = tokens(1)
      (name, value.toDouble)
    }


  /////////////////////// method 1 /////////////////////////////////
  def printTopMetrics() = {
    val sortedMetrics = readMetrics().sortBy(_._2).take(LIMIT)

    sortedMetrics.foreach(println)
  }


  ///////////////////// method 2 /////////////////////////////////
  def printTopMetricsI2I() = {

    val iteratorToIteratorTransformation = (records: Iterator[(String, Double)]) => {
      /*
      i2i transformation
      - they are NARROW TRANSFORMATION
      - Spark will "selectively" spill data to disk when partitions are too big for memory

      Warning : don't traverse more than once or convert to collections
     */

      implicit val ordering: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)
      val limitedCollection = new mutable.TreeSet[(String, Double)]()

      records.foreach { record =>
        limitedCollection.add(record)
        if (limitedCollection.size > LIMIT) {
          limitedCollection.remove(limitedCollection.last)
        }
      }
      // I've traversed the iterator
      limitedCollection.toIterator
    }

    val topMetrics = readMetrics()
      .mapPartitions(iteratorToIteratorTransformation)
      .repartition(1)
      .mapPartitions(iteratorToIteratorTransformation)

    val result = topMetrics.take(LIMIT)
    result.foreach(println)
  }

  /**
    * Exercises
    */

  def printTopMetricsEx1() = {
    /*
        Better than the "dummy (first approach)" approach
        - not sorting the entire RDD

        Bad (worse than the optimal)
        - sorting the entire partition
        - forcing the iterator in memory - this can OOM your executors

     */
    val topMetrics = readMetrics()
      .mapPartitions(_.toList.sortBy(_._2).take(LIMIT).toIterator)
      .repartition(1)
      .mapPartitions(_.toList.sortBy(_._2).take(LIMIT).toIterator)
      .take(LIMIT)

    topMetrics.foreach(println)
  }

  /*
    Better than ex1
    - extracting top 10 values per partition instead of sorting the entire partition

    Bad because
    - we are using (.toList) can forcing OOM your executors
    - iterating over the list twice
    - if the list is immutable, time spent allocating objects (and GC)
   */

  def printTopMetricsEx2() = {
    val topMetrics = readMetrics()
      .mapPartitions { records =>
        implicit val ordering: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)
        val limitedCollection = new mutable.TreeSet[(String, Double)]()

        records.toList.foreach { record =>
          limitedCollection.add(record)
          if (limitedCollection.size > LIMIT) {
            limitedCollection.remove(limitedCollection.last)
          }
        }
        // I've traversed the iterator
        limitedCollection.toIterator
      }
      .repartition(1)
      .mapPartitions { records =>
        implicit val ordering: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)
        val limitedCollection = new mutable.TreeSet[(String, Double)]()

        records.toList.foreach { record =>
          limitedCollection.add(record)
          if (limitedCollection.size > LIMIT) {
            limitedCollection.remove(limitedCollection.last)
          }
        }
        // I've traversed the iterator
        limitedCollection.toIterator
      }
      .take(LIMIT)
    topMetrics.foreach(println)
  }


  def main(args: Array[String]): Unit = {
    printTopMetrics()
    printTopMetricsI2I()
    Thread.sleep(1000000)
    //DataGenerator.generateMetrics("src/main/resources/generated/metrics/metrics10m.txt", 10000000)

  }
}
// To Remember

// I2I transformation : manipulate each partition with an iterator function

// Benefits
// - any transformation per-partition is a "NARROW" transformation.
// - if partitions are too large, Spark will spill to disk what can't fit in memory

// Anti-patterns
// - collecting the iterator in memory (specially collection conversion)
// - multiple passes over the data

// Lessons
// - traverse the iterator ONCE
// - don't use collection conversions





















