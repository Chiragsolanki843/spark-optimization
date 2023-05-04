package part3dfjoins

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object BroadcastJoins {

  val spark = SparkSession.builder()
    .appName("Broadcast Joins")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val sc = spark.sparkContext

  val rows = sc.parallelize(List(
    Row(0, "zero"),
    Row(1, "first"),
    Row(2, "second"),
    Row(3, "third")
  ))

  val rowsSchema = StructType(Array(
    StructField("id", IntegerType), // here column name is 'id'
    StructField("order", StringType)
  ))

  // small table
  val lookupTable: DataFrame = spark.createDataFrame(rows, rowsSchema)

  // large table
  val table = spark.range(1, 100000000) // here column name is 'id'

  // the innocent join
  val joined = table.join(lookupTable, "id")

  joined.explain()
  // joined.show() - takes an ice age
  /*
  == Physical Plan ==
  *(5) Project [id#6L, order#3]
  +- *(5) SortMergeJoin [id#6L], [cast(id#2 as bigint)], Inner
     :- *(2) Sort [id#6L ASC NULLS FIRST], false, 0
     :  +- Exchange hashpartitioning(id#6L, 200), true, [id=#27]
     :     +- *(1) Range (1, 100000000, step=1, splits=1)
     +- *(4) Sort [cast(id#2 as bigint) ASC NULLS FIRST], false, 0
        +- Exchange hashpartitioning(cast(id#2 as bigint), 200), true, [id=#33]
           +- *(3) Filter isnotnull(id#2)
              +- *(3) Scan ExistingRDD[id#2,order#3]
   */

  // a smarter join
  val joinedSmart = table.join(broadcast(lookupTable), "id")
  joinedSmart.explain()
  //joinedSmart.show()

  /*
  == Physical Plan ==
  *(2) Project [id#6L, order#3]
  +- *(2) BroadcastHashJoin [id#6L], [cast(id#2 as bigint)], Inner, BuildRight
     :- *(2) Range (1, 100000000, step=1, splits=1)
     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint))), [id=#64] //TODO it showing shuffle but data will not shuffle
        +- *(1) Filter isnotnull(id#2)
           +- *(1) Scan ExistingRDD[id#2,order#3]
   */
  // if we have small DF/RDDs and large DF/RDDs then we can join through 'broadcast' join
  // 100x perf!


  // auto-broadcast detection
  val bigTable = spark.range(1, 100000000)
  val smallTable = spark.range(1, 10000) // size estimated by spark - auto-broadcast
  val joinedNumbers = bigTable.join(smallTable, "id")

  // deactivate auto-broadcast
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) // 30 byte and by default it will 10MB

  joinedNumbers.explain()

  /* //TODO -- spark will automatically detected smaller table for joining so broadcast will happen and still if you use small table first or last but it will detect in broadcast
  == Physical Plan ==
  *(2) Project [id#12L]
  +- *(2) BroadcastHashJoin [id#12L], [id#14L], Inner, BuildRight
     :- *(2) Range (1, 100000000, step=1, splits=1)
     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false])), [id=#88]
        +- *(1) Range (1, 10000, step=1, splits=1)
   */
  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }
}

// Broadcast Join
// Scenario : joining small and large table

// Share the smaller DF/RDD across all executors
// tiny overhead
// all other operations done in memory

// pros
//  shuffles avoided
//  insane speed

// Risks
//  not enough driver memory
//  if smaller DF is quite big - large overhead
// if smaller DF is quite big - OOMing executors

// Broadcasting can be done automatically by spark

// finds one DF smaller than a threshold

// spark.sql.autoBroadcastJoinThreshold = 10485760 (10MB)  if you want to disable it just set '-1'
// also this is available for Dataframe API, spark will not so for RDD
