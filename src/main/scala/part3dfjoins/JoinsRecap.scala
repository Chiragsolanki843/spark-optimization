package part3dfjoins

import com.sun.xml.internal.bind.v2.TODO
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object JoinsRecap {

  val spark = SparkSession.builder()
    .appName("Joins Recap")
    .master("local[2]")
    .getOrCreate()

  val sc = spark.sparkContext

  def readFiles(fileName: String) =
    spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$fileName")

  val guitarsDF = readFiles("guitars")

  val guitaristsDF = readFiles("guitarPlayers")

  val bandsDF = readFiles("bands")

  // inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")

  val guitaristBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  // outer joins
  // left outer = everything in inner join + all the rows in the LEFT table, with nulls in the rows not passing the condition in the RIGHT table
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer = everything in inner join + all the rows in the RIGHT table, with nulls in the rows not passing the condition in the LEFT table
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // full outer join = everything in left_outer + right_outer
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi joins = everything in the left DF for which THERE IS a row in the right DF satisfying the condition
  // essentially a filter
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti join = everything in the left DF for which THERE IS NOT a row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")

  // cross join = everything in the left table with everything in the right table
  // dangerous : NRows (crossJoin) = NRows(left) x NRows(right)
  // careful with outer joins with non-unique keys

  // RDD joins
  val colorsScores = Seq(
    ("blue", 1),
    ("red", 4),
    ("green", 3),
    ("yellow", 5),
    ("orange", 2),
    ("cyan", 0)
  )

  val colorsRDD: RDD[(String, Int)] = sc.parallelize(colorsScores)
  val text = "The sky is blue, but the orange pale sun turns from yellow to red"
  val words = text.split(" ").map(_.toLowerCase()).map((_, 1)) // standard technique for counting words with RDDs
  val wordsRDD = sc.parallelize(words).reduceByKey(_ + _) // counting word occurrence
  val scores: RDD[(String, (Int, Int))] = wordsRDD.join(colorsRDD) // implied join type is inner
  val rightOuter = wordsRDD.rightOuterJoin(colorsRDD)

  def main(args: Array[String]): Unit = {

  }
}
