import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._

object MinTemperaturesDataset {

  case class Temperatures(stationID: String, date: Int, entryType: String, temperature: Float)
  // ITE00100554,18000101,TMAX,-75, v1, v2, E,

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Data has no headers - schema needs to be explicitly defined
    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("entryType", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)

    import spark.implicits._
    val ds = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .schema(temperatureSchema)
      .csv("res/1800.csv")
      .as[Temperatures]

    val minTempsByStation = ds
      .select("stationID", "temperature")
      .filter($"entryType" === "TMIN")
      .groupBy("stationId").min("temperature")

    // Convert to Celsius
    val minTempsByStationC = minTempsByStation
      .withColumn("temperature", round($"min(temperature)" * 0.1f, 2)) // withColumn replaces or creates a column
      .select("stationID", "temperature").sort("temperature")

    // Collect, format, and print the results
    val results = minTempsByStationC.collect()

    for (result <- results) {
      val station = result(0)
      val temp = result(1).asInstanceOf[Float]
      val formattedTemp = f"$temp%.2f C"
      println(s"$station minimum temperature: $formattedTemp")
    }

    spark.stop()

  }
}
