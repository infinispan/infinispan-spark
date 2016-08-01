package org.infinispan.spark.examples.util

import java.io.IOException
import java.lang.System.{getProperty => sys}
import java.util.concurrent.LinkedBlockingQueue

import com.twitter.hbc._
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.infinispan.spark.examples.twitter.Tweet
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json._

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Simple DStream connector to Twitter
  */
class TwitterDStream(@transient val ssc_ : StreamingContext,
                                   storage: StorageLevel = StorageLevel.MEMORY_ONLY)
        extends ReceiverInputDStream[Tweet](ssc_) {

   val ConsumerKeyProp = "twitter.oauth.consumerKey"
   val ConsumerSecretProp = "twitter.oauth.consumerSecret"
   val AccessTokenProp = "twitter.oauth.accessToken"
   val AccessTokenSecretProp = "twitter.oauth.accessTokenSecret"

   override def getReceiver() = new TwitterReceiver(storage, sys(ConsumerKeyProp), sys(ConsumerSecretProp),
      sys(AccessTokenProp), sys(AccessTokenSecretProp))

}

private[util] class TwitterReceiver(storageLevel: StorageLevel, consumerKey: String,
                                    consumerSecret: String, accessToken: String, accessTokenSecret: String)
        extends Receiver[Tweet](storageLevel) {

   private lazy val logger: Logger = LoggerFactory.getLogger(classOf[TwitterDStream])


   val ClientName = "Hosebird-client"
   val QueueSize = 10000
   val Timeout = 1 minute

   @transient lazy val OAuth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret)

   @transient lazy val blockingQueue = new LinkedBlockingQueue[String](QueueSize)

   @transient lazy val Endpoint = {
      val sampleEndpoint = new StatusesSampleEndpoint()
      sampleEndpoint.stallWarnings(false)
      sampleEndpoint
   }

   @transient lazy val client = new ClientBuilder()
           .name(ClientName)
           .hosts(Constants.STREAM_HOST)
           .endpoint(Endpoint)
           .authentication(OAuth)
           .processor(new StringDelimitedProcessor(blockingQueue))
           .build()

   @volatile var stopRequested = false

   override def onStart() = {
      logger.info("Starting receiver")
      client.connect()
      new Thread {
         setDaemon(true)

         override def run() = {
            while (!stopRequested) {
               if (client.isDone) {
                  throw new IOException("Client disconnected")
               }
               val message = blockingQueue.poll(Timeout.toSeconds, SECONDS)
               logger.info(s"Received message $message")
               val json = Json.parse(message)
               val delete = (json \ "delete").toOption
               if(delete.isEmpty) {
                  val id = json \ "id"
                  val retweet = json \ "retweet_count"
                  val text = json \ "text"
                  val user = json \ "user" \ "screen_name"
                  val country = json \ "place" \ "country"
                  store(new Tweet(id.as[Long], user.as[String], country.asOpt[String].getOrElse("N/A"), retweet.as[Int], text.as[String]))
               }
            }
         }
      }.start()
   }

   override def onStop() = {
      stopRequested = true
      client.stop()
   }
}

object TwitterDStream {

   def create(ssc: StreamingContext, storageLevel: StorageLevel) = new TwitterDStream(ssc, storageLevel)

   def create(jssc: JavaStreamingContext, storageLevel: StorageLevel) = new TwitterDStream(jssc.ssc, storageLevel)

   def create(ssc: StreamingContext) = new TwitterDStream(ssc)

   def create(jssc: JavaStreamingContext): JavaReceiverInputDStream[Tweet] = new TwitterDStream(jssc.ssc)
}
