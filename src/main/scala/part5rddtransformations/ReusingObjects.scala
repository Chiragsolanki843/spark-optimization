package part5rddtransformations

import generator.DataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReusingObjects {

  val spark = SparkSession.builder()
    .appName("Reusing JVM objects")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
    Analyze text
    Receive batches of text from data sources
    "35 // some text"

    Stats per each data source id:
    - the number of lines in total
    - total number of words in total
    - length of the longest word
    - the number of occurrences of the word "imperdiet"

    Result should be VERY FAST.
  */

  val textPath = "src/main/resources/generated/lipsum/3m.txt"

  val criticalWord = "imperdiet"

  val text = sc.textFile(textPath).map { line =>
    val tokens = line.split("//")
    (tokens(0), tokens(1))
  }

  def generateData() = {
    DataGenerator.generateText(textPath, 60000000, 3000000, 200)
  }


  //////////////////// Version 1
  case class TextStats(nLine: Int, nWords: Int, maxWordLength: Int, occurrences: Int)

  object TextStats {
    val zero = TextStats(0, 0, 0, 0)
  }


  def collectStats() = {

    def aggregateNewRecord(textStats: TextStats, record: String): TextStats = {
      val newWords = record.split(" ")
      val longestWord = newWords.maxBy(_.length)
      val newOccurrences = newWords.count(_ == criticalWord)
      TextStats(
        textStats.nLine + 1,
        textStats.nWords + newWords.length,
        if (longestWord.length > textStats.maxWordLength) longestWord.length else textStats.maxWordLength,
        textStats.occurrences + newOccurrences
      )
    }

    def combineStats(stats1: TextStats, stats2: TextStats): TextStats = {
      TextStats(
        stats1.nLine + stats2.nLine,
        stats1.nWords + stats2.nWords,
        if (stats1.maxWordLength > stats2.maxWordLength) stats1.maxWordLength else stats2.maxWordLength,
        stats1.occurrences + stats2.occurrences
      )
    }

    val aggregate: RDD[(String, TextStats)] = text.aggregateByKey(TextStats.zero)(aggregateNewRecord, combineStats)
    aggregate.collectAsMap()
  }

  /////////////////////// Version 2

  // if not Serializable then spark will throw the error while running in cluster
  class MutableTextStats(var nLine: Int, var nWords: Int, var maxWordLength: Int, var occurrences: Int) extends Serializable

  object MutableTextStats extends Serializable {
    def zero = new MutableTextStats(0, 0, 0, 0)
  }

  def collectStats2() = {

    def aggregateNewRecord(textStats: MutableTextStats, record: String): MutableTextStats = {
      val newWords = record.split(" ")
      val longestWord = newWords.maxBy(_.length)
      val newOccurrences = newWords.count(_ == criticalWord)

      textStats.nLine += 1
      textStats.nWords += newWords.length
      textStats.maxWordLength = if (longestWord.length > textStats.maxWordLength) longestWord.length else textStats.maxWordLength
      textStats.occurrences += newOccurrences

      textStats
    }

    def combineStats(stats1: MutableTextStats, stats2: MutableTextStats): MutableTextStats = {

      stats1.nLine += stats2.nLine
      stats1.nWords += stats2.nWords
      stats1.maxWordLength = if (stats1.maxWordLength > stats2.maxWordLength) stats1.maxWordLength else stats2.maxWordLength
      stats1.occurrences += stats2.occurrences

      stats1
    }

    val aggregate: RDD[(String, MutableTextStats)] = text.aggregateByKey(MutableTextStats.zero)(aggregateNewRecord, combineStats)
    aggregate.collectAsMap()
  }


  ////////////////////////////////// Version 3 - JVM arrays

  object UglyTextStats extends Serializable {

    val nLinesIndex = 0
    val nWordsIndex = 1
    val longestWordIndex = 2
    val occurrencesIndex = 3

    def aggregateNewRecord(textStats: Array[Int], record: String): Array[Int] = {
      val newWords = record.split(" ") // array of [String]

      var i = 0
      while (i < newWords.length) {
        val word = newWords(i)
        val wordLength = word.length

        textStats(longestWordIndex) = if (wordLength > textStats(longestWordIndex)) wordLength else textStats(longestWordIndex)
        textStats(occurrencesIndex) += (if (word == criticalWord) 1 else 0)

        i += 1
      }
      textStats(nLinesIndex) += 1
      textStats(nWordsIndex) += newWords.length

      textStats
    }

    def combineStats(stats1: Array[Int], stats2: Array[Int]): Array[Int] = {

      stats1(nLinesIndex) += stats2(nLinesIndex)
      stats1(nWordsIndex) += stats2(nWordsIndex)
      stats1(longestWordIndex) = if (stats1(longestWordIndex) > stats2(longestWordIndex)) stats1(longestWordIndex) else stats2(longestWordIndex)
      stats1(occurrencesIndex) += stats2(occurrencesIndex)

      stats1
    }
  }

  def collectStats3() = {

    // this method also need to serializable otherwise spark driver not compute the whole thing which we wrote
    // so spark driver need "Serializable" every method for compute
    val aggregate: RDD[(String, Array[Int])] = text.aggregateByKey(Array.fill(4)(0))(UglyTextStats.aggregateNewRecord, UglyTextStats.combineStats)
    aggregate.collectAsMap()
  }

  def main(ags: Array[String]): Unit = {
    //generateData()
    collectStats()
    collectStats2()
    collectStats3()

    Thread.sleep(1000000)
  }
}

// To Remember

// Reuse JVM objects by making them mutable

// Useful for scenario when
// - your aggregations/ data structures are not supported by spark out of the box
// - multiple passes over the data are not acceptable
// - performance is critical, i.e even a few seconds count so then you have to use this method

// Beware of
// - using tuples - they are (immutable) case classes
// - converting between Scala collections like (foreach, count, maxBy)- especially implicit conversions


















