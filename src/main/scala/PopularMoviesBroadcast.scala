import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}

object PopularMoviesBroadcast {

  case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  def loadMovieNames() : Map[Int, String] = {
    """Load up a Map of movie IDs to movie names."""

    implicit val codec: Codec = Codec("ISO-8859-1") // Handle character encoding issues

    var movieNames:Map[Int, String] = Map()         // Create a Map of ints to strings, and populate it from u.item

    val lines = Source.fromFile("res/ml-100k/u.item")
    for (line <- lines.getLines()) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()

    movieNames
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    val nameDict = spark.sparkContext.broadcast(loadMovieNames()) // create the broadcast variable

    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._
    val movies = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("res/ml-100k/u.data")
      .as[Movies]

    val movieCounts = movies.groupBy("movieID").count()

    // Create a user-defined function to look up movie names from our shared Map variable
    // We start by declaring an "anonymous function" in Scala
    // Then wrap it with a udf
    val lookupName: Int => String = (movieID: Int) => {   // takes an integer and returns string. Takes an integer movieID and passes through the block of code
      nameDict.value(movieID)
    }
    val lookupNameUDF = udf(lookupName)

    val moviesWithNames = movieCounts
      .withColumn("movieTitle", lookupNameUDF(col("movieID"))) // Add a movieTitle column using our new udf

    val sortedMoviesWithNames = moviesWithNames.sort("count")

    sortedMoviesWithNames.show(sortedMoviesWithNames.count.toInt, truncate = false)

    spark.stop()

  }

}

