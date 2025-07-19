import org.apache.log4j._
import org.apache.spark._
object SpentByCustomer {

  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val userID = fields(0).toInt
    val amount = fields(2).toFloat
    (userID, amount)
  }

  def main (args: Array[String])  {
    """Calculate the total amount spent by customer"""

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local", "SpentByCustomer")
    val input = sc.textFile("res/customer-orders.csv")
    val parsedLines = input.map(parseLine)
    val customerAmountSum = parsedLines.reduceByKey( (x,y) => x + y)
    val amountCustomer = customerAmountSum.map( x => (x._2, x._1) ).sortByKey()

    for (result <- amountCustomer) {
      val amount = result._1
      val customer = result._2
      println(s"$customer: $amount")
    }

  }

}
