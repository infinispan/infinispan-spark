package org.infinispan.spark.test

import java.lang.Thread._

import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverStarted}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, Suite}

/**
 * Trait to be mixed-in by tests requiring org.apache.spark.streaming.StreamingContext
 *
 * @author gustavonalle
 */
trait SparkStream extends BeforeAndAfterEach {
   this: Suite with RemoteTest =>

   protected var sc: SparkContext = _
   protected var ssc: StreamingContext = _

   private lazy val config: SparkConf = new SparkConf().setMaster("local[8]").setAppName(this.getClass.getName).set("spark.driver.host","127.0.0.1")

   override protected def beforeEach(): Unit = {
      sc = new SparkContext(config)
      ssc = new StreamingContext(sc, Seconds(1))
      super.beforeEach()
   }

   override protected def afterEach(): Unit = {
      ssc.stop(stopSparkContext = true)
      sc.stop()
      super.afterEach()
   }

   protected def executeAfterReceiverStarted(block: => Unit) = {
      ssc.addStreamingListener(new StreamingListener {
         override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
            sleep(1000)
            block
         }
      })
   }

}
