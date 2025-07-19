import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types._

object LinearRegressionDataframeDataset {

  case class RegressionSchema(label: Double, features_raw: Double)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("LinearRegressionDF")
      .master("local[*]")
      .getOrCreate()

    // Load up our page speed / amount spent data
    val regressionSchema = new StructType()
      .add("label", DoubleType, nullable = true)        // "label" is the value to predict
      .add("features_raw", DoubleType, nullable = true) // "feature" is the data used to make the prediction

    import spark.implicits._
    val dsRaw = spark.read
      .option("sep", ",")
      .schema(regressionSchema)
      .csv("res/regression.txt")
      .as[RegressionSchema]

    val assembler = new VectorAssembler().
      setInputCols(Array("features_raw")).
      setOutputCol("features")
    val df = assembler.transform(dsRaw)
      .select("label","features")

    // Split the data into training data and testing data
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)

    // Create the linear regression model
    val lir = new LinearRegression()
      .setRegParam(0.3)         // regularization
      .setElasticNetParam(0.8)  // elastic net mixing
      .setMaxIter(100)          // max iterations
      .setTol(1E-6)             // convergence tolerance

    // Train the model using our training data
    val model = lir.fit(trainingDF)

    // See if the values in the test data can be predicted
    // Generate predictions using our linear regression model for all features in the test dataframe.
    // This adds a "prediction" column to our testDF dataframe.
    val fullPredictions = model.transform(testDF).cache()

    // Extract the predictions and the "known" correct labels.
    val predictionAndLabel = fullPredictions.select("prediction", "label").collect()

    // Print out the predicted and actual values for each point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }

    spark.stop()

  }

}