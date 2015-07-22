package org.infinispan.spark.test

import java.time.{Duration => JDuration}
import java.util.concurrent.TimeUnit
import java.util.{List => JList}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import scala.annotation.meta.param
import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

/**
 * @author gustavonalle
 */
object StreamingUtils {

   class TestReceiver[T](of: Seq[T], streamItemEvery: Duration) extends Receiver[T](StorageLevel.MEMORY_ONLY) {
      override def onStart(): Unit = {
         of.foreach { item =>
            Thread.sleep(streamItemEvery.toMillis)
            store(item)
         }
      }

      override def onStop(): Unit = {}
   }

   class TestInputDStream[T: ClassTag](@(transient@param) ssc_ : StreamingContext, of: Seq[T], streamItemEvery: Duration) extends ReceiverInputDStream[T](ssc_) {
      override def getReceiver(): Receiver[T] = new TestReceiver[T](of, streamItemEvery)
   }

   def createJavaReceiverDInputStream[T](jssc: JavaStreamingContext, of: JList[T], streamItemEvery: JDuration) = {
      implicit val cmt: ClassTag[T] = implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
      JavaReceiverInputDStream.fromReceiverInputDStream(new TestInputDStream[T](jssc.ssc, of.toSeq, Duration(streamItemEvery.getNano, TimeUnit.NANOSECONDS)))
   }

}
