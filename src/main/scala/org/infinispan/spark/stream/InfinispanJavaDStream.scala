package org.infinispan.spark.stream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.api.java.{JavaDStream, JavaInputDStream, JavaPairDStream, JavaStreamingContext}
import org.infinispan.client.hotrod.event.ClientEvent
import org.infinispan.spark._
import org.infinispan.spark.config.ConnectorConfiguration

/**
  * @author gustavonalle
  */
object InfinispanJavaDStream {

   def writeToInfinispan[K, V](javaDStream: JavaPairDStream[K, V], configuration: ConnectorConfiguration) = {
      javaDStream.dstream.foreachRDD(rdd => rdd.writeToInfinispan(configuration))
   }

   def writeToInfinispan[K, V](javaDStream: JavaDStream[(K, V)], configuration: ConnectorConfiguration) = {
      javaDStream.dstream.foreachRDD(rdd => rdd.writeToInfinispan(configuration))
   }

   def createInfinispanInputDStream[K, V](javaStreamingContext: JavaStreamingContext,
                                          storageLevel: StorageLevel, configuration: ConnectorConfiguration, includeState: Boolean) = {
      val infinispanDStream: InfinispanInputDStream[K, V] = new InfinispanInputDStream[K, V](javaStreamingContext.ssc, storageLevel, configuration, includeState)
      JavaInputDStream.fromInputDStream(infinispanDStream)
   }

   def createInfinispanInputDStream[K, V](javaStreamingContext: JavaStreamingContext,
                                          storageLevel: StorageLevel, configuration: ConnectorConfiguration): JavaInputDStream[(K, V, ClientEvent.Type)] =
      createInfinispanInputDStream(javaStreamingContext, storageLevel, configuration, includeState = false)
}
