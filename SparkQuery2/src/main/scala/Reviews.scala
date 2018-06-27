import akka.dispatch.Mapper
import org.apache.spark.{SparkConf, SparkContext}


object Reviews {

  //class MapForReview extends Mapper

  def main(args: Array[String]): Unit = {

    val newConf = new SparkConf().setAppName("WordCount App").setMaster("local");
    // get the spark context with config settings from commandline
    val sc = new SparkContext(newConf);
    // get the input directories from command line
    val input1 = sc.textFile(args(0))
    val input2 = sc.textFile(args(1))

    // make sure to skip the header rows
    val movieData = input1.filter(row => row.contains("movieId") == false);
    val reviewData = input2.filter(row => row.contains("movieId") == false);

    // map movie titles with movieId
    val mappedMovie = movieData.map(line => {
      val lineArr = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

      // Format is [movieId, movieTitle]
      (lineArr(0), lineArr(1))
    })

    val mappedReviews = reviewData.map(line => {
      val lineArr = line.split(",")
      (lineArr(1), lineArr(2).toFloat)
    }).groupByKey().
      map(pair => (pair._1, (pair._2.sum.toFloat / pair._2.size.toDouble) + "\t" + pair._2.size)).
      filter((keyAndValues) => {
        val values = keyAndValues._2.toString.split("\t")
        val avg = values(0).toFloat
        val count = values(1).toInt
        avg > 4.0 && count > 10
      })


    // Sorts by movieId, ratingAvg, numRatings:  Fortunately, since it is by alphabetical order we
    // can just do a string match instead of a comparator
    val joinedData = mappedReviews.join(mappedMovie).sortBy(_._2)

    // valArray._2 has the title and valArray._1 contains the "rating, numRatings"
    // final output format: (title averageRating numRatings)
    val res = joinedData.values.map((valArray) => {
      valArray._2 + "\t" + valArray._1

    })

    // write to file
    res.saveAsTextFile(args(2))

  }
}
