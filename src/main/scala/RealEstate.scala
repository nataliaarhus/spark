import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.types._

object RealEstate {

  case class RegressionSchema(No: Integer, TransactionDate: Double, HouseAge: Double,
                              DistanceToMRT: Double, NumberConvenienceStores: Integer, Latitude: Double,
                              Longitude: Double, PriceOfUnitArea: Double)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("LinearRegressionDF")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val dsRaw = spark.read
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("res/realestate.csv")
      .as[RegressionSchema]

    val assembler = new VectorAssembler().
      setInputCols(Array("HouseAge", "DistanceToMRT", "NumberConvenienceStores")).
      setOutputCol("features")
    val df = assembler.transform(dsRaw)
      .select("PriceOfUnitArea","features")

    // Split the data into training data and testing data
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)

    // Create the decision tree
    val lir = new DecisionTreeRegressor()
      .setFeaturesCol("features")
      .setLabelCol("PriceOfUnitArea")

    // Train the model using our training data
    val model = lir.fit(trainingDF)

    // See if the values in the test data can be predicted
    // Generate predictions using our linear regression model for all features in the test dataframe.
    // This adds a "prediction" column to our testDF dataframe.
    val fullPredictions = model.transform(testDF).cache()

    // Extract the predictions and the "known" correct labels.
    val predictionAndLabel = fullPredictions.select("prediction", "PriceOfUnitArea").collect()

    // Print out the predicted and actual values for each point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }

    spark.stop()

  }

}
