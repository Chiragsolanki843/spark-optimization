package part4rddjoins

import generator.DataGenerator
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SimpleRDDJoins {

  val spark = SparkSession.builder()
    .appName("RDD Joins")
    .master("local[*]") // here need to put '*' bcz parallelization is very imp for this lesson
    .getOrCreate()

  val sc = spark.sparkContext

  val rootFolder = "src/main/resources/generated/examData"

  //DataGenerator.generateExamData(rootFolder, 1000000, 5) // TODO before run the application remove or comment this line bcz data will generate again or give error if already there.

  def readIds() = sc.textFile(s"$rootFolder/examIds.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1)) // (examId, Name)
    }
    .partitionBy(new HashPartitioner(10))

  def readExamScores() = sc.textFile(s"$rootFolder/examScores.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1).toDouble) // (examID, AttemptScore)
    }

  // goal : the number of students who passed the exam (= at least one attempt > 9.0)

  def plainJoin() = {
    val candidates = readIds()
    val scores = readExamScores()

    //simple join
    val joined: RDD[(Long, (Double, String))] = scores.join(candidates) // (score attempt, candidate name)
    val finalScores = joined
      .reduceByKey((pair1, pair2) =>
        if (pair1._1 > pair2._1) pair1
        else pair2)
      .filter(_._2._1 > 9.0)

    finalScores.count()
  }

  def preAggregate() = {
    val candidates = readIds()
    val scores = readExamScores()

    // do aggregation first - 10% perf increase
    val maxScores: RDD[(Long, Double)] = scores.reduceByKey(Math.max)
    val finalScores = maxScores.join(candidates)
      .filter(_._2._1 > 9.0)
    finalScores.count()
  }

  def preFiltering() = {
    val candidates = readIds()
    val scores = readExamScores()

    // do filtering first before join
    val maxScores = scores.reduceByKey(Math.max).filter(_._2 > 9.0)
    val finalScores = maxScores.join(candidates)

    finalScores.count()
  }

  def coPartitioning() = {

    val candidates = readIds()
    val scores = readExamScores()

    val partitionerForScores = candidates.partitioner match {
      case none => new HashPartitioner(candidates.getNumPartitions)
      case Some(partitioner) => partitioner
    }
    val repartitionedScores = scores.partitionBy(partitionerForScores)
    val joined: RDD[(Long, (Double, String))] = repartitionedScores.join(candidates) // (score attempt, candidate name)
    val finalScores = joined
      .reduceByKey((pair1, pair2) =>
        if (pair1._1 > pair2._1) pair1
        else pair2)
      .filter(_._2._1 > 9.0)

    finalScores.count()
  }

  def combined() = {
    val candidates = readIds()
    val scores = readExamScores()

    val partitionerForScores = candidates.partitioner match {
      case none => new HashPartitioner(candidates.getNumPartitions)
      case Some(partitioner) => partitioner
    }
    val repartitionedScores = scores.partitionBy(partitionerForScores)

    // do filtering first before join
    val maxScores = repartitionedScores.reduceByKey(Math.max).filter(_._2 > 9.0)
    val finalScores = maxScores.join(candidates)

    finalScores.count()
  }

  def main(args: Array[String]): Unit = {

    plainJoin()
    preAggregate()
    preFiltering()
    coPartitioning()
    combined()
    Thread.sleep(1000000)

  }
}

// Remember
// Spark doesn't optimize our operations the same as it did with SQL
//  - no query plans
//  - no column pruning
//  - no pre-filtering

// We-re in control :
//  - pre-filter the data before the join
//  - pre-aggregate (even partial) results before the join
//  - co-partition : assign partitioner to RDDs so we can avoid shuffles TODO --> pay attention all the time in your job. for co-partitioning  ... Danial recommended
//  - combine all the above!


















