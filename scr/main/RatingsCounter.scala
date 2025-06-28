package mypkg

import org.apache.spark._
import org.apache.log4j._

object RatingsCounter {
  """Count up how many of each star rating exists in the MovieLens 100K data set."""

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR) // Set the log level to only print errors

    val sc = new SparkContext("local[*]", "RatingsCounter") // Create a SparkContext using every core of the local machine, named RatingsCounter

    val lines = sc.textFile("res/ml-100k/u.data") // Load up each line of the ratings data into an RDD

    val ratings = lines.map(x => x.split("\t")(2)) // Convert each line to a string, split it out by tabs, and extract the third field (The file format is userID, movieID, rating, timestamp)

    val results = ratings.countByValue()  // Count up how many times each value (rating) occurs

    val sortedResults = results.toSeq.sortBy(_._1) // Sort the resulting map of (rating, count) tuples

    sortedResults.foreach(println) // Print each result on its own line.
  }
}
