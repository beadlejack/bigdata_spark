import org.apache.spark.{SparkConf, SparkContext}

object Popularity {
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

    // map movieId with movieTitle
    val mappedMovie = movieData.map(line => {
      val lineArr = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

      // Format is (movieId, movieTitle)
      (lineArr(0), lineArr(1))
    })

    // map movieId with reviewCount
    // Format is (movieId, reviewCount)
    val mappedReviews = reviewData.map(line => (line.split(",")(1),1))
      .reduceByKey((x,y) => x + y)

    // join mapped results on movieId
    val joinedData = mappedReviews.join(mappedMovie).sortBy(_._2)

    // final output format: (reviewCount title)
    val res = joinedData.values.map((valArray) => {
      valArray._1 + "\t" + valArray._2
    })

    // write to file
    res.saveAsTextFile(args(2))

  }
}
