package sampleApp1

import org.apache.spark.SparkContext

object TEst extends App {
  val sc= new SparkContext("local[*]","Movie Recommendation App")


  var keyValue=Seq((1,"A"),(1,"B"),(2,"C"),(3,"D"))

  var rdd = sc.parallelize(keyValue)

  var joined = rdd.join(rdd)

  joined.foreach(println)



}
