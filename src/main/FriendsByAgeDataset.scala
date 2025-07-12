import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FriendsByAgeDataset {

  case class Person(id: Int, name: String, age: Int, friends: Long)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("res/fakefriends.csv")
      .as[Person]

    // Use SQL commands to transform data
    val friendsByAge = ds.select("age", "friends")
    friendsByAge.groupBy("age").avg("friends").show()
    friendsByAge.groupBy("age").avg("friends").sort("age").show()
    friendsByAge.groupBy("age").agg(round(avg("friends"), 2))
      .sort("age").show()
    friendsByAge.groupBy("age").agg(round(avg("friends"), 2)
      .alias("friends_avg")).sort("age").show()

    spark.stop()

  }

}
