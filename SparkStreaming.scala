//Imports
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.math.Ordering

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder


//For sentiment analysis
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.util.CoreMap
import scala.collection.JavaConversions._
import java.util.Properties


//Function to get if the tweet is positive. Will return true if the tweet is positive.
def getSentiment(tweetString : String): Boolean = {
  val props: Properties = new Properties()
  props.put("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)
  val annotation: Annotation = new Annotation(tweetString)
  pipeline.annotate(annotation)
  val sentences1: List[CoreMap] = annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).toList
  val sentiments = sentences1
      .map(sentence => (sentence, sentence.get(classOf[SentimentAnnotatedTree])))
      .map { case (sentence, tree) => RNNCoreAnnotations.getPredictedClass(tree) }
  (sentiments.sum.toDouble / sentiments.length) > 1
}


//Twitter credentials
System.setProperty("twitter4j.oauth.consumerKey", "EC1S5tpwZa4PGY8pguIorPEpH")
System.setProperty("twitter4j.oauth.consumerSecret", "dFckMkJRj4wTxGUk25Kqqog90Ba10RWmpeZWqE7j1HKjKPqaTj")
System.setProperty("twitter4j.oauth.accessToken", "4882207543-tQvB9hzjgCCZxsWJ9tAH5dtadDcjvWHycJD1poj")
System.setProperty("twitter4j.oauth.accessTokenSecret", "X4AjSlmiIMs0qtbQlI9PidtvJP5oporslB9fvYsaAMSdL")


// Directory to output
val outputDirectory = "/twitter"

// Recompute the counts every 1 minute
val slideInterval = new Duration(1 * 60 * 1000)

// Compute counts for the last 3 minutes
val windowLength = new Duration(3 * 60 * 1000)

// Wait 30 minutes before stopping the streaming job
val timeoutJobLength = 1 * 30 * 60 * 1000


dbutils.fs.rm(outputDirectory, true)


var newContextCreated = false
var num = 0
val iphonePattern = "(^|\\W)iphone ?x(\\W|$)".r

// This is the function that creates the SteamingContext and sets up the Spark Streaming job.
def creatingFunc(): StreamingContext = {
  // Create a Spark Streaming Context.
  val ssc = new StreamingContext(sc, slideInterval)
  // Create a Twitter Stream for the input source. 
  val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  val twitterStream = TwitterUtils.createStream(ssc, auth)
  // Gather only tweets which have iphone x in them.
  val tweetStream = twitterStream.map(_.getText).filter(text => iphonePattern.findFirstIn(text.toLowerCase) != None).map(getSentiment)
  // Compute the counts of each positives and negatives by window.
  val windowedTweetCountStream = tweetStream.map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, windowLength, slideInterval)

  // For each window, calculate the total, positives and negative tweets
  windowedTweetCountStream.foreachRDD(tweetCountRDD => {
    val total = tweetCountRDD.values.sum()
    val pos = tweetCountRDD.filter{case (isPositive, value) => isPositive}.values.sum()
    val windowString = "Number of tweets containing the word iPhone X: " + total + "\nNumber of positive tweets: " + pos + "\nNumber of negative tweets: " + (total - pos)
    dbutils.fs.put(s"${outputDirectory}/top_tweets_${num}", windowString, true)
    println(s"------ Tweets For window ${num}")
    println(windowString)
    num = num + 1
  })
  
  newContextCreated = true
  ssc
}


@transient val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

ssc.start()
ssc.awaitTerminationOrTimeout(timeoutJobLength)

StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }


display(dbutils.fs.ls(outputDirectory))


dbutils.fs.head(s"${outputDirectory}/top_tweets_20")

