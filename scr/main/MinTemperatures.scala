import org.apache.spark._
import org.apache.log4j._
import scala.math.min
object MinTemperatures {

  def parseLine(line:String): (String, String, Float) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f
    (stationID, entryType, temperature)
  }

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MinTemperatures")
    val lines = sc.textFile("res/1800.csv")
    val parsedLines = lines.map(parseLine) // Convert to (stationID, entryType, temperature) tuples
    val minTemps = parsedLines.filter(x => x._2 == "TMIN") // Filter out all but TMIN entries
    val stationTemps = minTemps.map(x => (x._1, x._3.toFloat)) // Leave only the 1st and 3rd column (stationID, temperature)
    val minTempsByStation = stationTemps.reduceByKey((x, y) => min(x, y)) // Reduce by stationID retaining the minimum temperature found
    val results = minTempsByStation.collect() // Collect, format, and print the results
    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temperature: $formattedTemp")
    }

  }

}
