package part3dfjoins

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Bucketing {
  // Bucketing split DF into chunks and its available only in Dataframe writer and file base data sources


  val spark = SparkSession.builder()
    .appName("Bucketing")
    .master("local")
    //.config("spark.sql.autoBroadcastJoinThreshold", -1) --> we can use here as well
    .getOrCreate()

  import spark.implicits._

  // deactivated Broadcasting
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  val large = spark.range(1, 1000000).selectExpr("id * 5 as id").repartition(10)
  val small = spark.range(1, 10000).selectExpr("id * 3 as id").repartition(3)

  val joined = large.join(small, "id")

  joined.explain()
  /*
  == Physical Plan ==  TODO classical kind of joining in spark
  *(5) Project [id#2L]
  +- *(5) SortMergeJoin [id#2L], [id#6L], Inner
     :- *(2) Sort [id#2L ASC NULLS FIRST], false, 0
     :  +- Exchange hashpartitioning(id#2L, 200), true, [id=#40]
     :     +- Exchange RoundRobinPartitioning(10), false, [id=#39]
     :        +- *(1) Project [(id#0L * 5) AS id#2L]
     :           +- *(1) Range (1, 1000000, step=1, splits=1)
     +- *(4) Sort [id#6L ASC NULLS FIRST], false, 0
        +- Exchange hashpartitioning(id#6L, 200), true, [id=#47]
           +- Exchange RoundRobinPartitioning(3), false, [id=#46]
              +- *(3) Project [(id#4L * 3) AS id#6L]
                 +- *(3) Range (1, 10000, step=1, splits=1)
   */

  // bucketing
  large.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_large1") // it will save into Spark Warehouse directory means in project folder

  small.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_small1")
  // bucketing and storing DF into disk its very expensive, it's almost expensive like regular shuffle

  val bucktedLarge = spark.table("bucketed_large1")
  val bucketedSmall = spark.table("bucketed_small1")

  val bucketedJoined = bucktedLarge.join(bucketedSmall, "id")
  bucketedJoined.explain() // it's really very very fast for joining
  /*
  == Physical Plan ==
  *(3) Project [id#11L]
  +- *(3) SortMergeJoin [id#11L], [id#13L], Inner
     :- *(1) Sort [id#11L ASC NULLS FIRST], false, 0
     :  +- *(1) Project [id#11L]
     :     +- *(1) Filter isnotnull(id#11L)
     :        +- *(1) ColumnarToRow
     :           +- FileScan parquet default.bucketed_large[id#11L] Batched: true, DataFilters: [isnotnull(id#11L)], Format: Parquet, Location: InMemoryFileIndex[file:/D:/Data%20Engineering/spark-optimization-master/spark-warehouse/bucketed_..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
     +- *(2) Sort [id#13L ASC NULLS FIRST], false, 0
        +- *(2) Project [id#13L]
           +- *(2) Filter isnotnull(id#13L)
              +- *(2) ColumnarToRow
                 +- FileScan parquet default.bucketed_small[id#13L] Batched: true, DataFilters: [isnotnull(id#13L)], Format: Parquet, Location: InMemoryFileIndex[file:/D:/Data%20Engineering/spark-optimization-master/spark-warehouse/bucketed_..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
   */

  // bucketing for groups
  val flightsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/flights/flights.json")
    .repartition(2)

  val mostDelayed = flightsDF
    .filter("origin = 'DEN' and arrdelay > 1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last) // TODO --> '$' convert string to column

  mostDelayed.explain()
  /*
  == Physical Plan ==
  *(4) Sort [avg(arrdelay)#53 DESC NULLS LAST], true, 0
  +- Exchange rangepartitioning(avg(arrdelay)#53 DESC NULLS LAST, 200), true, [id=#111]
     +- *(3) HashAggregate(keys=[origin#27, dest#24, carrier#18], functions=[avg(arrdelay#17)])
        +- Exchange hashpartitioning(origin#27, dest#24, carrier#18, 200), true, [id=#107]
           +- *(2) HashAggregate(keys=[origin#27, dest#24, carrier#18], functions=[partial_avg(arrdelay#17)])
              +- Exchange RoundRobinPartitioning(2), false, [id=#103]
                 +- *(1) Project [arrdelay#17, carrier#18, dest#24, origin#27]
                    +- *(1) Filter (((isnotnull(origin#27) AND isnotnull(arrdelay#17)) AND (origin#27 = DEN)) AND (arrdelay#17 > 1.0))
                       +- FileScan json [arrdelay#17,carrier#18,dest#24,origin#27] Batched: false, DataFilters: [isnotnull(origin#27), isnotnull(arrdelay#17), (origin#27 = DEN), (arrdelay#17 > 1.0)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Data Engineering/spark-optimization-master/src/main/resources/data/fli..., PartitionFilters: [], PushedFilters: [IsNotNull(origin), IsNotNull(arrdelay), EqualTo(origin,DEN), GreaterThan(arrdelay,1.0)], ReadSchema: struct<arrdelay:double,carrier:string,dest:string,origin:string>

   */

  //  flightsDF.write
  //    .partitionBy("origin")
  //    .bucketBy(4, "dest", "carrier")
  //    .saveAsTable("flights_bucketed") // just as long as a shuffle
  //
  //  val flightsBucketed = spark.table("flights_bucketed")
  //  val mostDelayed2 = flightsBucketed
  //    .filter("origin = 'DEN' and arrdelay > 1")
  //    .groupBy("origin", "dest", "carrier")
  //    .avg("arrdelay")
  //    .orderBy($"avg(arrdelay)".desc_nulls_last)
  //
  //  mostDelayed2.explain()

  /*
  == Physical Plan == TODO --> no hashpartitioning required means no shuffle
  *(2) Sort [avg(arrdelay)#148 DESC NULLS LAST], true, 0
  +- Exchange rangepartitioning(avg(arrdelay)#148 DESC NULLS LAST, 200), true, [id=#165]
     +- *(1) HashAggregate(keys=[origin#122, dest#119, carrier#113], functions=[avg(arrdelay#112)])
        +- *(1) HashAggregate(keys=[origin#122, dest#119, carrier#113], functions=[partial_avg(arrdelay#112)])
           +- *(1) Project [arrdelay#112, carrier#113, dest#119, origin#122]
              +- *(1) Filter (isnotnull(arrdelay#112) AND (arrdelay#112 > 1.0))
                 +- *(1) ColumnarToRow
                    +- FileScan parquet default.flights_bucketed[arrdelay#112,carrier#113,dest#119,origin#122] Batched: true, DataFilters: [isnotnull(arrdelay#112), (arrdelay#112 > 1.0)], Format: Parquet, Location: InMemoryFileIndex[file:/D:/Data%20Engineering/spark-optimization-master/spark-warehouse/flights_b..., PartitionFilters: [isnotnull(origin#122), (origin#122 = DEN)], PushedFilters: [IsNotNull(arrdelay), GreaterThan(arrdelay,1.0)], ReadSchema: struct<arrdelay:double,carrier:string,dest:string>, SelectedBucketsCount: 4 out of 4

   */

  /**
    * Bucket Pruning
    * */

  val the10 = bucktedLarge.filter(col("id") === 10)
  the10.show()
  the10.explain()
  /*
  == Physical Plan ==
  *(1) Project [id#11L]
  +- *(1) Filter (isnotnull(id#11L) AND (id#11L = 10))
     +- *(1) ColumnarToRow
        +- FileScan parquet default.bucketed_large1[id#11L] Batched: true, DataFilters: [isnotnull(id#11L), (id#11L = 10)], Format: Parquet, Location: InMemoryFileIndex[file:/D:/Data%20Engineering/spark-optimization-master/spark-warehouse/bucketed_..., PartitionFilters: [], PushedFilters: [IsNotNull(id), EqualTo(id,10)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 1 out of 4
   */

  def main(args: Array[String]): Unit = {
    // joined.count() // 4-5s took for compute
    // bucketedJoined.count() // 4s 0for bucketing + 0.5s for counting took for compute
    // mostDelayed.show() // ~1second
    //mostDelayed2.show() // ~0.2s = 5x perf!

  }
}

// to Remember

//  - Split the data by the columns you will later use for joins
//  - n buckets per partition

// Almost as expensive as a regular shuffle
// Subsequent joins will be much faster and will not incur shuffles if we use columns which is we store in bucket

// Same benefit for groupBy
// - pre-bucket your DFs by the columns you will use for grouping
// - subsequent groupings will be much faster

// Bonus : bucket pruning
//  - Spark will only search a subset of your DF when you filter by the column you bucketed by

