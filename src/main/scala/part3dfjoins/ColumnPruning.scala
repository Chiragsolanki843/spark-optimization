package part3dfjoins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnPruning {

  val spark = SparkSession.builder()
    .appName("Column Pruning")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val sc = spark.sparkContext

  import spark.implicits._

  def readFile(fileName: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$fileName")

  val guitarsDF = readFile("guitars/guitars.json")

  val guitarPlayersDF = readFile("guitarPlayers/guitarPlayers.json")

  val bandsDF = readFile("bands/bands.json")

  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")

  val guitaristBandsDF = guitarPlayersDF.join(bandsDF, joinCondition, "inner")

  guitaristBandsDF.explain()

  /*
  == Physical Plan ==
  *(2) BroadcastHashJoin [band#22L], [id#38L], Inner, BuildLeft
  :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#34]
  :  +- *(1) Project [band#22L, guitars#23, id#24L, name#25] TODO<-- UNNECESSARY select everything in DFs column
  :     +- *(1) Filter isnotnull(band#22L)
  :        +- FileScan json [band#22L,guitars#23,id#24L,name#25] Batched: false, DataFilters: [isnotnull(band#22L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Data Engineering/spark-optimization-master/src/main/resources/data/gui..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,guitars:array<bigint>,id:bigint,name:string>
  +- *(2) Project [hometown#37, id#38L, name#39, year#40L] TODO it will select every DFs so this will not optimal
     +- *(2) Filter isnotnull(id#38L)
        +- FileScan json [hometown#37,id#38L,name#39,year#40L] Batched: false, DataFilters: [isnotnull(id#38L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Data Engineering/spark-optimization-master/src/main/resources/data/bands], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<hometown:string,id:bigint,name:string,year:bigint>
   */

  val guitaristWithoutBands = guitarPlayersDF.join(bandsDF, joinCondition, "left_anti")

  guitaristWithoutBands.explain()

  /*
  == Physical Plan ==
  *(2) BroadcastHashJoin [band#22L], [id#38L], LeftAnti, BuildRight
  :- FileScan json [band#22L,guitars#23,id#24L,name#25] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/D:/Data Engineering/spark-optimization-master/src/main/resources/data/gui..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<band:bigint,guitars:array<bigint>,id:bigint,name:string>
  +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#63]
     +- *(1) Project [id#38L] TODO <- COLUMN PRUNING  it will just select band id not select entire column
        +- *(1) Filter isnotnull(id#38L)
           +- FileScan json [id#38L] Batched: false, DataFilters: [isnotnull(id#38L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Data Engineering/spark-optimization-master/src/main/resources/data/bands], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>

   Column pruning = cut off columns that are not relevant
   = shrinks DF // shrinks the DF size
  ** useful for joins and groups (groupBy)
   */

  // project and filter pushDown
  val namesDF = guitaristBandsDF.select(guitarPlayersDF.col("name"), bandsDF.col("name")) // joined DF and differentiated by original DF column

  namesDF.explain()
  /*
  == Physical Plan ==
  *(2) Project [name#25, name#39]
  +- *(2) BroadcastHashJoin [band#22L], [id#38L], Inner, BuildRight
     :- *(2) Project [band#22L, name#25] TODO<- COLUMN PRUNING
     :  +- *(2) Filter isnotnull(band#22L)
     :     +- FileScan json [band#22L,name#25] Batched: false, DataFilters: [isnotnull(band#22L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Data Engineering/spark-optimization-master/src/main/resources/data/gui..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,name:string>
     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#100]
        +- *(1) Project [id#38L, name#39]
           +- *(1) Filter isnotnull(id#38L)
              +- FileScan json [id#38L,name#39] Batched: false, DataFilters: [isnotnull(id#38L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Data Engineering/spark-optimization-master/src/main/resources/data/ban..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,name:string>

   Spark tends to drop columns as early as possible.
   Should be YOUR goal as well.
   */

  val joinedCondition1 = array_contains(guitarPlayersDF.col("guitars"), guitarsDF.col("id"))

  val rockDF = guitarPlayersDF
    .join(bandsDF, joinCondition)
    .join(guitarsDF, joinedCondition1)

  val essentialsDF = rockDF.select(
    guitarPlayersDF.col("name"),
    bandsDF.col("name"),
    upper(guitarsDF.col("make")) //Converts a string column to upper case.
  )
  essentialsDF.explain()

  /*
  == Physical Plan ==
  *(3) Project [name#25, name#39, upper(make#9) AS upper(make)#147] TODO -> the upper function is done LAST
  +- BroadcastNestedLoopJoin BuildRight, Inner, array_contains(guitars#23, id#8L)
     :- *(2) Project [guitars#23, name#25, name#39]
     :  +- *(2) BroadcastHashJoin [band#22L], [id#38L], Inner, BuildRight
     :     :- *(2) Project [band#22L, guitars#23, name#25] TODO <- COLUMN PRUNING
     :     :  +- *(2) Filter isnotnull(band#22L)
     :     :     +- FileScan json [band#22L,guitars#23,name#25] Batched: false, DataFilters: [isnotnull(band#22L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Data Engineering/spark-optimization-master/src/main/resources/data/gui..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,guitars:array<bigint>,name:string>
     :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#154]
     :        +- *(1) Project [id#38L, name#39] TODO <- COLUMN PRUNING
     :           +- *(1) Filter isnotnull(id#38L)
     :              +- FileScan json [id#38L,name#39] Batched: false, DataFilters: [isnotnull(id#38L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Data Engineering/spark-optimization-master/src/main/resources/data/ban..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,name:string>
     +- BroadcastExchange IdentityBroadcastMode, [id=#144]
     // TODO i use method that's why project is not showing is i read DF regular mode then it will show project with selected columns
        +- FileScan json [id#8L,make#9] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/D:/Data Engineering/spark-optimization-master/src/main/resources/data/gui..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:bigint,make:string>
   */

  /**
    * LESSION : if you anticipate that the joined table is much larger than the table on whose column you are applying the
    * map-side operation, e.g. " * 5 ", or "upper", do this operation on the small table FIRST.
    * Particularly useful for outer joins.
    */
  def main(args: Array[String]): Unit = {

  }
}

// Column Pruning

// Spark selects just the relevant columns after a join

// if you do a select after a join, the Project operation is pushed to joined DFs

// Further map-side operations can be manually pushed down
//  if we anticipate the joined DF is bigger than either side

// Spark sometimes can't prune columns automatically
// good practice : select just the right columns ourselves before join

// Most benefits seen in massive datasets

















