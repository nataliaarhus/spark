// Before running this script, modify the run configuration to pass the user ID as a CLI argument.

import org.apache.log4j._
import org.apache.spark.ml.recommendation._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

import scala.collection.mutable

object MovieRecommendationsALSDataset {

  case class MoviesNames(movieId: Int, movieTitle: String)    // Row format to feed into ALS
  case class Rating(userID: Int, movieID: Int, rating: Float) // Get movie name by given dataset and id
  def getMovieName(movieNames: Array[MoviesNames], movieId: Int): String = {
    val result = movieNames.filter(_.movieId == movieId)(0)

    result.movieTitle
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("ALSExample")
      .master("local[*]")
      .getOrCreate()


    println("Loading movie names...")
    // Define the schema for u.item
    val moviesNamesSchema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("movieTitle", StringType, nullable = true)

    // Define the schema for u.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._
    // Load the movie names data as dataset
    val names = spark.read
      .option("sep", "|")
      .option("charset", "ISO-8859-1")
      .schema(moviesNamesSchema)
      .csv("res/ml-100k/u.item")
      .as[MoviesNames]

    val namesList = names.collect() // copy all names to memory on the driver script

    // Load up movie data as dataset
    val ratings = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("res/ml-100k/u.data")
      .as[Rating]

    // Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model...")

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userID")
      .setItemCol("movieID")
      .setRatingCol("rating")

    val model = als.fit(ratings)

    // Get top-10 recommendations for the user we specified
    val userID:Int = args(0).toInt
    val users = Seq(userID).toDF("userID")
    val recommendations = model.recommendForUserSubset(users, 10)

    // Display them (oddly, this is the hardest part!)
    println("\nTop 10 recommendations for user ID " + userID + ":")

    for (userRecs <- recommendations) {
      val myRecs = userRecs(1)                                    // First column is userID, second is the recs
      val temp = myRecs.asInstanceOf[mutable.WrappedArray[Row]]   // Tell Scala what it is
      for (rec <- temp) {
        val movie = rec.getAs[Int](0)
        val rating = rec.getAs[Float](1)
        val movieName = getMovieName(namesList, movie)
        println(movieName, rating)
      }
    }

    spark.stop()

  }

}