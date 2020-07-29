package sparkALS

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.recommendation.{ALS, ALSModel}

object Recommender extends App{

  val sc = new SparkContext("local[*]","ALS Based Recommendation App")

  val spark = SparkSession.builder.
    master("local[*]")
    .appName("ALS Based Recommendation App")
    .getOrCreate()


  val model = ALSModel.load(s"D:\\Zone24x7\\Recommendation App\\Recommendation.model")

  // Generate top 10 movie recommendations for each users
  val userRecords = model.recommendForAllUsers(10)
  userRecords.take(5).foreach(println)

//  Generate top 10 similar movies to each movie
  val movieRecords = model.recommendForAllItems(10)
  movieRecords.take(5).foreach(println)

}
