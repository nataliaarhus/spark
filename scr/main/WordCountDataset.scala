import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountDataset {

  case class Book(value: String) // default column name "value"

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val input = spark.read.text("res/book.txt").as[Book]

    // Split using a regular expression that extracts words
    val words = input
      .select(explode(split($"value", "\\W+")).alias("word")) // default column name "value"
      .filter($"word" =!= "") // is not null
    val lowercaseWords = words.select(lower($"word").alias("word"))
    val wordCounts = lowercaseWords.groupBy("word").count()
    val wordCountsSorted = wordCounts.sort("count")
    wordCountsSorted.show(wordCountsSorted.count.toInt)

    // ANOTHER WAY TO DO IT (Blending RDDs and Datasets)
    // val bookRDD = spark.sparkContext.textFile("res/book.txt")
    // val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))
    // val wordsDS = wordsRDD.toDS()
    //  ...

    spark.stop()

  }

}
