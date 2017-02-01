package org.infinispan.spark

import java.util.Properties

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.streaming.dstream.DStream
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.impl.ConfigurationProperties.SERVER_LIST
import org.infinispan.spark.rdd.RemoteCacheManagerBuilder

package object stream {

   implicit class InfinispanDStream[K, V](stream: DStream[(K, V)]) {

      private def getCacheManager(configuration: Properties): RemoteCacheManager = {
         val rcm = RemoteCacheManagerBuilder.create(configuration)
         stream.context.sparkContext.addSparkListener(new SparkListener {
            override def onJobEnd(jobEnd: SparkListenerJobEnd) = rcm.stop()
         })
         rcm
      }

      def writeToInfinispan(configuration: Properties) = {
         val rcm = getCacheManager(configuration)
         val cache = getCache(configuration, rcm)
         val topologyConfig = getCacheTopology(cache.getCacheTopologyInfo)
         configuration.put(SERVER_LIST, topologyConfig)
         stream.foreachRDD(_.writeToInfinispan(configuration))
      }
   }

}
