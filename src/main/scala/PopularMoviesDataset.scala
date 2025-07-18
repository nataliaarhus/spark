import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object PopularMoviesDataset {

  final case class Movie(movieID: Int)
  // Dataset doesn't have to include all columns as the Dataframe. This we we only get the movie ID colum

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._
    val moviesDS = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("res/ml-100k/u.data")
      .as[Movie]

    val topMovieIDs = moviesDS.groupBy("movieID").count().orderBy(desc("count"))
    topMovieIDs.show(10)

    spark.stop()

  }

}

