package part2foundations

import org.apache.spark.sql.{SaveMode, SparkSession}

object TestDeployApp {


  // TestDeployApp inputFile outputFile
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Need input file and output File")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Test Deploy App")
      //.config("spark.master", "local[2]") in this program we run spark app in docker so
      // master will not set to 'local'

      // method 1 (configure spark application from the code)
      .config("spark.executor.memory", "1g")
      .getOrCreate()

    import spark.implicits._
    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(args(0))

    val goodComediesDF = moviesDF.select(
      $"Title",
      $"IMDB_Rating".as("Rating"),
      $"Release_Date".as("Release")
    )
      .where(($"Major_Genre" === "Comedy") and ($"IMDB_Rating" > 6.5))
      .orderBy($"Rating".desc_nulls_last)

    // method 2
    spark.conf.set("spark.executor.memory", "1g") // warning - not all configurations available
    // because it common-sense while cluster running  not worth to set and configurations.

    goodComediesDF.show()

    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))
  }
}
// commands
// docker compose up --scale spark-worker = 3
// /spark/bin/spark-submit
// --class part2foundations.TestDeployApp
// --master spark://55bb031ccd11:7077  //docker id
// --conf spark.executor.memory 1g or --executor.memory 1g (Optional) method 3
// --deploy-mode client
// --verbose
// --supervise
// /opt/spark-apps/spark-optimization.jar /opt/spark-data/movies.json /opt/spark-data/goodComedies.json

