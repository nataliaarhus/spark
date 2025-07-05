import org.apache.spark._
import org.apache.log4j._

object WordCount {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local", "WordCount")
    val input = sc.textFile("res/book.txt")
    // val words = input.flatMap(x => x.split(" "))                                 // splits all words by a space character
    val words = input.flatMap(x => x.split("\\W+"))                         // split by regular expression (word)
    val lowercaseWords = words.map(x => x.toLowerCase())

    // val wordCounts = lowercaseWords.countByValue()                               // count up the occurrences of each word
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y ) // count occurrences in a distributed way
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()         // change the order of word and value to sort by key

    // wordCounts.foreach(println)
    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }

  }

}
