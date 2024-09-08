import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("MLPipelineBlogDemo").master("local[*]").getOrCreate()

    //Reduce the logging noise.
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Read the data
    val df = spark.read.option("header", "true").option("inferSchema", true).csv("Social_Network_Ads.csv")
    df.printSchema()

    //Split Data

    val splits = df.randomSplit(Array(0.8, 0.2), seed = 1234L)
    val train = splits(0)
    val test = splits(1)

    //Configure Pipeline

    val genderIndexer = new StringIndexer().setInputCol("Gender").setOutputCol("GenderIndex")
    val genderOneHotEncoder = new OneHotEncoder().setInputCol("GenderIndex").setOutputCol("GenderOHE")

    val features = Array("GenderOHE", "Age", "EstimatedSalary")
    val dependetVariable = "Purchased"

    val vectorAssembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
    val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures")

    val logisticRegression = new LogisticRegression()
      .setFeaturesCol("scaledFeatures")
      .setLabelCol(dependetVariable)

    // Assemble the pipeline
    val stages = Array(genderIndexer, genderOneHotEncoder, vectorAssembler, scaler, logisticRegression)
    val pipeline = new Pipeline().setStages(stages)

    //Fit the pipeline
    val model = pipeline.fit(train)

    //Predicting Result for test set
    val results = model.transform(test)

    // Evaluating the Result
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol(dependetVariable)
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    val accuracy = evaluator.evaluate(results)

    println(s"Accuracy of Model : ${accuracy}")
  }
}
