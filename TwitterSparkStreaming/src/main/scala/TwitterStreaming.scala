import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by pradyumnad on 07/07/15.
 */
object TwitterStreaming {

  def main(args: Array[String]) {


    val filters = args

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials

    System.setProperty("twitter4j.oauth.consumerKey", "9ChNjJGZlf9UwsZpPbBR2HDP0")
    System.setProperty("twitter4j.oauth.consumerSecret", "0A9tdSAjuMTuMhdTH5sjlSQmKkb6R3tACkeFLCORXXCDEfOc7z")
    System.setProperty("twitter4j.oauth.accessToken", "187084317-T64XUNek7p5zc9AJFJzRRP3gcwNgShkz7XXJ7JIr")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "mKvSEYJ22Dqe24tHDf6o25gb4154qByGRSxQg2nGqwYyc")

    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("STweetsApp").setMaster("local[*]")
    //Create a Streaming COntext with 2 second window
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets
    val stream = TwitterUtils.createStream(ssc, None, filters)
stream.print()
    //Map : Retrieving Hash Tags
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    //Finding the top hash Tags on 30 second window

    //Finding the top hash Tgas on 10 second window
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // Print popular hashtags


    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)


      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}

    })
     ssc.start()

    ssc.awaitTermination()
  }
}
