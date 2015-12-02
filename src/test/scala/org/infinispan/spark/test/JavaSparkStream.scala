package org.infinispan.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.infinispan.spark.serializer.JBossMarshallingSerializer
import org.scalatest.{BeforeAndAfterEach, Suite}

/**
 * Trait to be mixed-in by tests requiring org.apache.spark.streaming.api.java.JavaStreamingContext
 */
trait JavaSparkStream extends BeforeAndAfterEach {
   this: Suite with RemoteTest =>

   private lazy val config: SparkConf = new SparkConf().setMaster("local[4]")
           .setAppName(this.getClass.getName)
           .set("spark.serializer", classOf[JBossMarshallingSerializer].getName)

   protected var jssc: JavaStreamingContext = _
   protected var jsc: JavaSparkContext = _

   override protected def beforeEach(): Unit = {
      jsc = new JavaSparkContext(config)
      jssc = new JavaStreamingContext(jsc, Seconds(1))
      getRemoteCache.clear()
      super.beforeEach()
   }

   override protected def afterEach(): Unit = {
      jssc.stop(stopSparkContext = true)
      jsc.stop()
      super.afterEach()
   }

}
