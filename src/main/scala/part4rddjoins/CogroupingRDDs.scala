package part4rddjoins

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CogroupingRDDs {

  val spark = SparkSession.builder()
    .appName("Cogrouping RDDs")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
    Take all the student attempts
    - if a student passed (at least one attempt > 9.0), send them an email "PASSED"
    - else send them an email with "FAILED"
  * */

  val rootFolder = "src/main/resources/generated/examData"

  def readIds() = sc.textFile(s"$rootFolder/examIds.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1))
    }
    .partitionBy(new HashPartitioner(10))

  def readExamScores() = sc.textFile(s"$rootFolder/examScores.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1).toDouble)
    }

  def readExamEmails() = sc.textFile(s"$rootFolder/examEmails.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1))
    }

  def plainJoin() = {
    val scores = readExamScores().reduceByKey(Math.max)
    val candidates = readIds()
    val emails = readExamEmails()

    val results = candidates
      .join(scores) // RDD[(Long, (String, Double))]
      .join(emails) // RDD[(Long,((String, Double), String))]
      .mapValues {
        case ((name, maxAttempt), email) => // 'name' is not in use so we can place '_'
          if (maxAttempt >= 9.0) (email, "PASSED")
          else (email, "FAILED")
      }

    results.count()
  }

  def coGroupedJoin() = {
    val scores = readExamScores().reduceByKey(Math.max)
    val candidates = readIds()
    val emails = readExamEmails()

    // coGroup function will share the same partition across the RDDs to store data so it will 25% faster than plain join
    val result: RDD[(Long, Option[(String, String)])] = candidates.cogroup(scores, emails) // co-partition the 3 RDDs (name:String,score:Double,email:String) // :RDD[(Long,(Iterable[String],Iterable[Double],Iterable[String]))]
      .mapValues {
        case (nameIterable, maxAttemptIterable, emailIterable) =>
          val name = nameIterable.headOption
          val maxScore = maxAttemptIterable.headOption
          val email = emailIterable.headOption

          for {
            e <- email
            s <- maxScore
          } yield (e, if (s >= 9.0) "PASSED" else "FAILED")
      }
    result.count()
    result.count() // it will take less than 1s for compute bcz RDDs are in sharing partition
  }

  def main(args: Array[String]): Unit = {
    plainJoin()
    coGroupedJoin()
    Thread.sleep(1000000)
  }
}


// To Remember : coGroup

// Make sure ALL the RDDs share the same partitioner


// Particularly useful for multi-way joins (it will do fullOuter Join between the multiple RDDs)
// - All RDDs are shuffled 'at most once'
// - RDDs are never shuffled again if co-grouped RDD is reused

// Keeps the entire data - equivalent to a full outer join
// - for each key, an iterator of values is given
// - if there is no value for a "column", the respective iterator is empty





















