package org.infinispan.spark

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.streaming.dstream.DStream
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.rdd.RemoteCacheManagerBuilder

package object stream {

   implicit class InfinispanDStream[K, V](stream: DStream[(K, V)]) {

      private def getCacheManager(configuration: ConnectorConfiguration): RemoteCacheManager = {
         val rcm = RemoteCacheManagerBuilder.create(configuration)
         stream.context.sparkContext.addSparkListener(new SparkListener {
            override def onJobEnd(jobEnd: SparkListenerJobEnd) = rcm.stop()
         })
         rcm
      }

      def writeToInfinispan(configuration: ConnectorConfiguration) = {
         val rcm = getCacheManager(configuration)
         val cache = getCache(configuration, rcm)
         val topologyConfig = getCacheTopology(cache.getCacheTopologyInfo)
         configuration.setServerList(topologyConfig)
         stream.foreachRDD(_.writeToInfinispan(configuration))
      }
   }

}
