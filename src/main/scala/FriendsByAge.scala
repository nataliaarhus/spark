import org.apache.log4j._
import org.apache.spark._

/** Compute the average number of friends by age in a social network. */
object FriendsByAge {

  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String): (Int, Int) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")

    val lines = sc.textFile("res/fakefriends-noheader.csv")  // Load each line of the source data into an RDD

    val rdd = lines.map(parseLine) // Use our parseLines function to convert to (age, numFriends) tuples

    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE [age: numFriends]
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1) [age: (numFriends, 1)]
    // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by adding together all the numFriends values and 1's respectively.
    // [age: total of all friends, total instances]
    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2)) // x is the running total, y is the next records

    // Now we have tuples of (age, (totalFriends, totalInstances)). To compute the average we divide totalFriends / totalInstances for each age.
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    val results = averagesByAge.collect() // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)

    results.sorted.foreach(println)
  }

}
  