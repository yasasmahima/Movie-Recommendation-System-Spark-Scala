import java.nio.charset.CodingErrorAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.io.{Codec, Source}


object MovieRecommendations {

  val spark= new SparkContext("local[*]","Movie Recommendation App")

//  Method to map userId with Movie and Its Rating
  def userIdAndMovieRatings(): RDD[(Int,(Int,Double))]= {
    val rddFromFile: RDD[String] =spark.textFile("D:\\Zone24x7\\Recommendation App\\src\\main\\resources\\u.data")

    val mapedUserIdAndMovieRating: RDD[(Int, (Int, Double))] =rddFromFile.map(line =>{
      val fields=line.split("\\s+")
      (fields(0).toInt,(fields(1).toInt,fields(2).toDouble))
    })
    mapedUserIdAndMovieRating
  }

//  Method to filter duplicates in joined rdd(Movies with same UserId)
  def filterDuplicateMovies(userIdAndPairOfMovies:(Int,((Int,Double),(Int,Double)))): Boolean = {
    val id1: Int =userIdAndPairOfMovies._2._1._1
    val id2: Int =userIdAndPairOfMovies._2._2._1
    id1<id2
  }

//  Method to map similar user Id movies with their ratings
  def mapMoviesWithRatings(userIdAndMovieData:(Int,((Int,Double),(Int,Double)))): ((Int,Int),(Double,Double))= {
    val id1=userIdAndMovieData._2._1._1
    val id2=userIdAndMovieData._2._2._1

    val rating1=userIdAndMovieData._2._1._2
    val rating2=userIdAndMovieData._2._2._2

    ((id1,id2),(rating1,rating2))
  }

// Method to make CosineSimilarityMatrix - Find Similarity between two vectors
  def getCosineSimilarityMatrix(pairWiseMovieRatings: Iterable[(Double, Double)]): (Double,Int) = {
    var numberOfMoviePairs: Int =0
    var sumXX: Double =0.0
    var sumYY: Double =0.0
    var sumXY: Double =0.0

    for(ratingPair: (Double, Double) <- pairWiseMovieRatings){
      val ratingMovie1: Double =ratingPair._1
      val ratingMovie2: Double =ratingPair._2

      sumXX+=ratingMovie1*ratingMovie1
      sumYY+=ratingMovie2*ratingMovie2
      sumXY+=ratingMovie1*ratingMovie2
      numberOfMoviePairs+=1
    }

//    CosQ = A.B / |A||B|
    val numerator: Double =sumXY
    val denominator: Double =Math.sqrt(sumXX)*Math.sqrt(sumYY)
    val result: Double =numerator/denominator
    (result,numberOfMoviePairs)
  }


//  Method to map names to each movie ID
  def mapMovieIdAndName():Map[Int,String]={

    // If there is a character encoding issues could be handle by below
    implicit val code_encode = Codec("UTF-8")
    code_encode.onMalformedInput(CodingErrorAction.REPLACE)
    code_encode.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val movieIdAndName: Map[Int, String] = Source.fromFile("D:\\Zone24x7\\Recommendation App\\src\\main\\resources\\u.item").getLines().map(line =>{
      val lines=line.split('|')
      (lines.head.toInt,lines(1))
    }).toMap[Int,String]

    movieIdAndName
  }

  def filterAccordingToThreshold(moviePairAndScore: ((Int, Int), (Double, Int)),movieId:Int):Boolean= {

    val scoreThreshold: Double = 0.97 //Calculated for data set (Cosine value)(1 means exactly the same movie)
    val numberOfOccurences: Double = 50.0 // Depends on data set (Number of similar pairs)

    val moviePair: (Int, Int) = moviePairAndScore._1
    val ratingAndNumOfPairs: (Double, Int) = moviePairAndScore._2
    (moviePair._1 == movieId || moviePair._2 == movieId) && ratingAndNumOfPairs._1 > scoreThreshold && ratingAndNumOfPairs._2 > numberOfOccurences

  }

//  Def method to get top 10 related movie to given
  def getTop10Movies(moviesAndSimilarityScore: RDD[((Int, Int), (Double, Int))], movieId:Int): Unit = {

    val scoreThreshold: Double = 0.97
    val coOccurenceThreshold: Double = 50.0


    val moviePairsFilteredAccordingToThreshold: RDD[((Int, Int), (Double, Int))] = moviesAndSimilarityScore.filter((moviePairAndScore: ((Int, Int), (Double, Int))) => {
     filterAccordingToThreshold(moviePairAndScore,movieId)
    })

    val first10MoviesAndTheirScores: Array[((Int, Int), (Double, Int))] =moviePairsFilteredAccordingToThreshold.sortByKey().take(10)

    val idAndMovieNames=mapMovieIdAndName()

    println("Loading top 10 similar movies ------->")
    println("Top 10 movies related to "+idAndMovieNames(movieId)+" are : ")

    var count:Int=1;

    first10MoviesAndTheirScores.foreach(moviePairAndScore =>{
      val movie1: Int =moviePairAndScore._1._1
      val movie2: Int =moviePairAndScore._1._2
      var suggestedMovie: Int =movie2

      if(movie2==movieId) {
        suggestedMovie = movie1
      }
      println(count+") "+idAndMovieNames(suggestedMovie))
      count+=1;
    })
  }

  def main(args: Array[String]): Unit = {

//    Mapped movies with UserId
    val userIdMappedWithMovieIdAndRatings: RDD[(Int, (Int, Double))] =userIdAndMovieRatings()

//    Join Movies with Same User Id
    val moviesWithSameUserId: RDD[(Int, ((Int, Double), (Int, Double)))] =userIdMappedWithMovieIdAndRatings.join(userIdMappedWithMovieIdAndRatings)

//    Remove Duplicates
    val moviesWithSameUserIdWithoutDuplicates: RDD[(Int, ((Int, Double), (Int, Double)))] =moviesWithSameUserId.filter(filterDuplicateMovies)

//    Map same user ID movie pairs with Ratings
    val pairOfMoviesWithRatings: RDD[((Int, Int), (Double, Double))] =moviesWithSameUserIdWithoutDuplicates.map(mapMoviesWithRatings)

//    Group Pair of Movies with Rating by Key
    val groupPairOfMovies: RDD[((Int, Int), Iterable[(Double, Double)])] =pairOfMoviesWithRatings.groupByKey()

//    Ger Similarity Score for each movie pairs using CosineSimilarity Matrix
    val cosineSimilarityScoreForEachPair: RDD[((Int, Int), (Double, Int))] =groupPairOfMovies.mapValues(getCosineSimilarityMatrix)

      print("Enter the Movie Id : ")
      val movieId: Int = scala.io.StdIn.readLine.toInt

      getTop10Movies(cosineSimilarityScoreForEachPair,movieId)

  }

}