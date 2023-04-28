package part1recap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkRecap {

  // the entry point to the Spark Structured API (DF, DS)
  val spark = SparkSession.builder()
    .appName("Spark Recap")
    .master("local[2]")
    .getOrCreate()

  // read a DF
  val cars = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars")


  import spark.implicits._

  // select
  val usefulCarsData = cars.select(
    col("Name"), // column object
    $"Year" // another column object (needs spark implicits)
      (col("Weight_in_lbs") / 2.2).as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // 'expr' and 'selectExpr' we can pass 'SQL' like string

  val carsWeights = cars.selectExpr("Weight_in_lbs / 2.2")

  // filter
  val europeanCars = cars.filter(col("Origin") =!= "USA")
  //val europeanCars = cars.where(col("Origin") =!= "USA") // above and this are identical

  // aggregations
  val averageHP = cars.select(
    avg(col("Horsepower")).as("average_hp"),
    sum(col("Horsepower").as("sumOfHorsepower"))
  ) // sum, mean, stddev, min, max

  // grouping
  val countByOrigin = cars
    .groupBy(col("Origin")) // a RelationalGroupedDataset
    .count()

  // joining
  val guitarPlayers = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers")

  val bands = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands")

  val joincondition = guitarPlayers.col("band") === bands.col("id")
  val guitaristsBands = guitarPlayers.join(bands, joincondition, "inner")

  /*
      join types
      - inner join : only matching rows are kept
      - left/right/full outer join
      - semi/anti join
  */

  // datasets --> type [] distributed collection of JVM Object
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  val guitarPlayerDS = guitarPlayers.as[GuitarPlayer] // this conversion only happen when import spark.implicits._

  guitarPlayerDS.map(_.name)

  // Spark SQL
  cars.createOrReplaceTempView("cars")

  val americanCars = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin)

  // low-level API: RDDs -- type [] distributed collection of JVM Object except they don't have SQL API as DF and DS
  val sc = spark.sparkContext
  val numberRDD: RDD[Int] = sc.parallelize(1 to 1000000)

  // functional operators
  val doubles = numberRDD.map(_ * 2)

  // RDD -> DF
  val numbersDF = numberRDD.toDF("number") // if we convert RDD to DF then we lose type[] info, but we gain SQL capabilities

  // RDD --> DS
  val numbersDS = spark.createDataset(numberRDD)

  // DS --> RDD
  val guitarPlayersRDD = guitarPlayerDS.rdd

  // DF --> RDD
  val carsRDD = cars.rdd // RDD[row]

  def main(args: Array[String]): Unit = {
    // showing a DF to the console
    cars.show()
    cars.printSchema()


  }
}




























