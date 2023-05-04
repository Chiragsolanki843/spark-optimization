package part3dfjoins

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object PrePartitioning {

  val spark = SparkSession.builder()
    .appName("Pre-Partitioning")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // deactivate broadcast joins
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  /*
      addColumns(initialTable, 3) => dataframe with columns "id", "newCol1","newCol2" ,"newCol3"

   */

  def addColumns[T](df: Dataset[T], n: Int): DataFrame = {
    val newColumns = (1 to n).map(i => s"id * $i as newCol$i")
    df.selectExpr(("id" +: newColumns): _*)
  }


  // don't touch this
  val initialTable = spark.range(1, 10000000).repartition(10) // RoundRobinPartitioning(10)

  val narrowTable = spark.range(1, 5000000).repartition(7) // RoundRobinPartitioning(7)

  // scenario 1
  val wideTable = addColumns(initialTable, 30)
  val join1 = wideTable.join(narrowTable, "id")
  join1.explain()
  //println(join1.count())
  /*
  == Physical Plan == TODO this scenario took 20 sec for compute
      *(6) Project [id#0L, newCol1#8L, newCol2#9L, newCol3#10L, newCol4#11L, newCol5#12L, newCol6#13L, newCol7#14L, newCol8#15L, newCol9#16L, newCol10#17L, newCol11#18L, newCol12#19L, newCol13#20L, newCol14#21L, newCol15#22L, newCol16#23L, newCol17#24L, newCol18#25L, newCol19#26L, newCol20#27L, newCol21#28L, newCol22#29L, newCol23#30L, ... 7 more fields]
      +- *(6) SortMergeJoin [id#0L], [id#4L], Inner
         :- *(3) Sort [id#0L ASC NULLS FIRST], false, 0
         :  +- Exchange hashpartitioning(id#0L, 200), true, [id=#39]
         :     +- *(2) Project [id#0L, (id#0L * 1) AS newCol1#8L, (id#0L * 2) AS newCol2#9L, (id#0L * 3) AS newCol3#10L, (id#0L * 4) AS newCol4#11L, (id#0L * 5) AS newCol5#12L, (id#0L * 6) AS newCol6#13L, (id#0L * 7) AS newCol7#14L, (id#0L * 8) AS newCol8#15L, (id#0L * 9) AS newCol9#16L, (id#0L * 10) AS newCol10#17L, (id#0L * 11) AS newCol11#18L, (id#0L * 12) AS newCol12#19L, (id#0L * 13) AS newCol13#20L, (id#0L * 14) AS newCol14#21L, (id#0L * 15) AS newCol15#22L, (id#0L * 16) AS newCol16#23L, (id#0L * 17) AS newCol17#24L, (id#0L * 18) AS newCol18#25L, (id#0L * 19) AS newCol19#26L, (id#0L * 20) AS newCol20#27L, (id#0L * 21) AS newCol21#28L, (id#0L * 22) AS newCol22#29L, (id#0L * 23) AS newCol23#30L, ... 7 more fields]
         :        +- Exchange RoundRobinPartitioning(10), false, [id=#35]
         :           +- *(1) Range (1, 10000000, step=1, splits=1)
         +- *(5) Sort [id#4L ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(id#4L, 200), true, [id=#46]
               +- Exchange RoundRobinPartitioning(7), false, [id=#45]
                  +- *(4) Range (1, 5000000, step=1, splits=1)
   */

  // scenario 2   // TODO -> two table have same partitioner so data will not shuffle
  val altNarrow = narrowTable.repartition($"id") // TODO -> use a HashPartitioner
  val altInitial = initialTable.repartition($"id") // TODO -> use a HashPartitioner
  // TODO  -- join on co-partitioned DFs
  val join2 = altInitial.join(altNarrow, "id")
  val result2 = addColumns(join2, 30)
  join2.explain()

  //println(result2.count())
  /*
  == Physical Plan == TODO this scenario took 6sec for compute
      *(5) Project [id#0L]
      +- *(5) SortMergeJoin [id#0L], [id#4L], Inner
         :- *(2) Sort [id#0L ASC NULLS FIRST], false, 0
         :  +- Exchange hashpartitioning(id#0L, 200), false, [id=#91]
         :     +- *(1) Range (1, 10000000, step=1, splits=1)
         +- *(4) Sort [id#4L ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(id#4L, 200), false, [id=#97]
               +- *(3) Range (1, 5000000, step=1, splits=1)

   */
  /**
    * Lesson : Partition early.
    * Partitioning late is AT BEST what Spark naturally does.
    */

  // Scenario 3
  val enhanceColumnsFirst = addColumns(initialTable, 30)
  val repartitionedNarrow = narrowTable.repartition($"id")
  val repartitionedEnhanced = enhanceColumnsFirst.repartition($"id") // USELESS!
  val result3 = repartitionedEnhanced.join(repartitionedNarrow, "id")
  //println(result3.count()) // TODO around took 19-20seconds for compute
  result3.explain()
  /*
      == Physical Plan ==
      *(6) Project [id#0L, newCol1#166L, newCol2#167L, newCol3#168L, newCol4#169L, newCol5#170L, newCol6#171L, newCol7#172L, newCol8#173L, newCol9#174L, newCol10#175L, newCol11#176L, newCol12#177L, newCol13#178L, newCol14#179L, newCol15#180L, newCol16#181L, newCol17#182L, newCol18#183L, newCol19#184L, newCol20#185L, newCol21#186L, newCol22#187L, newCol23#188L, ... 7 more fields]
      +- *(6) SortMergeJoin [id#0L], [id#4L], Inner
         :- *(3) Sort [id#0L ASC NULLS FIRST], false, 0
         :  +- Exchange hashpartitioning(id#0L, 200), false, [id=#154]
         :     +- *(2) Project [id#0L, (id#0L * 1) AS newCol1#166L, (id#0L * 2) AS newCol2#167L, (id#0L * 3) AS newCol3#168L, (id#0L * 4) AS newCol4#169L, (id#0L * 5) AS newCol5#170L, (id#0L * 6) AS newCol6#171L, (id#0L * 7) AS newCol7#172L, (id#0L * 8) AS newCol8#173L, (id#0L * 9) AS newCol9#174L, (id#0L * 10) AS newCol10#175L, (id#0L * 11) AS newCol11#176L, (id#0L * 12) AS newCol12#177L, (id#0L * 13) AS newCol13#178L, (id#0L * 14) AS newCol14#179L, (id#0L * 15) AS newCol15#180L, (id#0L * 16) AS newCol16#181L, (id#0L * 17) AS newCol17#182L, (id#0L * 18) AS newCol18#183L, (id#0L * 19) AS newCol19#184L, (id#0L * 20) AS newCol20#185L, (id#0L * 21) AS newCol21#186L, (id#0L * 22) AS newCol22#187L, (id#0L * 23) AS newCol23#188L, ... 7 more fields]
         :        +- Exchange RoundRobinPartitioning(10), false, [id=#150]
         :           +- *(1) Range (1, 10000000, step=1, splits=1)
         +- *(5) Sort [id#4L ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(id#4L, 200), false, [id=#160]
               +- *(4) Range (1, 5000000, step=1, splits=1)

   */

  /**
    * Exercise : what would happen if we just repartitioned the smaller table to 10 partitions?
    * TERRIBLE!
    */

  initialTable.join(narrowTable.repartition(10), "id").explain() // identical to scenario 1

  /*
  == Physical Plan ==
  *(5) Project [id#0L]
  +- *(5) SortMergeJoin [id#0L], [id#4L], Inner
     :- *(2) Sort [id#0L ASC NULLS FIRST], false, 0
     :  +- Exchange hashpartitioning(id#0L, 200), true, [id=#207]
     :     +- Exchange RoundRobinPartitioning(10), false, [id=#206]
     :        +- *(1) Range (1, 10000000, step=1, splits=1)
     +- *(4) Sort [id#4L ASC NULLS FIRST], false, 0
        +- Exchange hashpartitioning(id#4L, 200), true, [id=#214]
           +- Exchange RoundRobinPartitioning(10), false, [id=#213]
              +- *(3) Range (1, 5000000, step=1, splits=1)
   */
  def main(args: Array[String]): Unit = {
    Thread.sleep(100000)
  }
}

// TO Remember

// Partition your data early so that Spark doesn't have to
//  - make the joined DFs share the same partitioner, e.g.partition by the same column
//  - decorate the joined DF later (especially if you have lots of transformations)

// Partitioning late is bad
//  - at best  : same perf as Spark out-of-the-box
//  - at worst : worse performance that not partitioning at all