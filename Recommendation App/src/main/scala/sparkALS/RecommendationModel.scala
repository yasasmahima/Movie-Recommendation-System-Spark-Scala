package sparkALS

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.tuning.{TrainValidationSplit,ParamGridBuilder}

object RecommendationModel extends App {

  val sc = new SparkContext("local[*]","ALS Based Recommendation App")

  val spark = SparkSession.builder.
    master("local[*]")
    .appName("ALS Based Recommendation App")
    .getOrCreate()

  var ratingData = spark.read.option("header","true").option("inferSchema","true").format("csv")
  .load("D:\\Zone24x7\\Recommendation App\\src\\main\\resources\\ratings.csv")

//Describe columns and data types in the CSV
  ratingData.printSchema()

//  Describe data set
  ratingData.describe().show()

//  Remove null values from the data set
  ratingData = ratingData.na.drop()

//  Remove Unwanted timestamp column
  ratingData = ratingData.drop("timestamp")

//  Als Model
  var als = new ALS()
    .setMaxIter(5)
    .setUserCol("userId")
    .setItemCol("movieId")
    .setRatingCol("rating")
    .setRegParam(0.01)
    .setColdStartStrategy("drop")

//  Split Data for train and Testing
  var splits=ratingData.randomSplit(Array(0.8,0.2),seed = 10)
  var trainingSet=splits(0)
  var testingSet=splits(1)


//  Fit Data to the pipe line
  var model=als.fit(trainingSet)


//  Get Predictions
  var predictions=model.transform(testingSet)

//  Evaluate Model
  val evaluator = new RegressionEvaluator()
    .setMetricName("mse")
    .setLabelCol("rating")
    .setPredictionCol("prediction")

  var mse = evaluator.evaluate(predictions)
  println(s"Mean Squared Error = $mse")

  model.save("Recommendation.model")

//  Parameterized optimization for the model
  als = new ALS()
    .setMaxIter(5)
    .setUserCol("userId")
    .setItemCol("movieId")
    .setRatingCol("rating")
    .setRegParam(0.01)
    .setColdStartStrategy("drop")

//  Set 9 different combinations of the two parameters
  var paramGrid = new ParamGridBuilder()
    .addGrid(als.rank,Array(5,10,15))
    .addGrid(als.regParam,Array(1,0.1,0.01))
    .build()

//   Split the data with 90 training, 10 validation
//   Pass in the parameter grid to search over
  var trainValidationSplit =  new TrainValidationSplit()
      .setEstimator(als)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(evaluator)
      .setTrainRatio(0.9)
      .setParallelism(4)

  var optimizedModel = trainValidationSplit.fit(trainingSet)

  var bestModel = optimizedModel.bestModel

  var bestPredictions = bestModel.transform(testingSet)

  var  newMse = evaluator.evaluate(predictions)
  println(s"Mean Squared Error after optimizing = $newMse")

  optimizedModel.save("Optimized.model")

}
