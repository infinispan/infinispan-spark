package org.infinispan

import java.net.InetSocketAddress

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.infinispan.client.hotrod.{CacheTopologyInfo, RemoteCacheManager}
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.rdd.RemoteCacheManagerBuilder

import scala.collection.JavaConversions._

package object spark {

   def getCacheTopology(cacheTopology: CacheTopologyInfo) = {
      val segmentsPerServer = cacheTopology.getSegmentsPerServer
      segmentsPerServer.keySet.map {
         case i: InetSocketAddress => s"${i.getHostString}:${i.getPort}"
      }.mkString(";")
   }

   def getCache[K, V](config: ConnectorConfiguration, rcm: RemoteCacheManager) = {
      val cacheName = config.getCacheName
      Option(cacheName).map(name => rcm.getCache[K, V](name)).getOrElse(rcm.getCache[K, V])
   }

   implicit class RDDExtensions[K, V](rdd: RDD[(K, V)]) extends Serializable {

      def writeToInfinispan(configuration: ConnectorConfiguration): Unit = {
         val processor = (ctx: TaskContext, iterator: Iterator[(K, V)]) => {
            val remoteCacheManager = RemoteCacheManagerBuilder.create(configuration)
            val cache = getCache[K, V](configuration, remoteCacheManager)
            configuration.setServerList(getCacheTopology(cache.getCacheTopologyInfo))
            ctx.addTaskCompletionListener(ctx => remoteCacheManager.stop())
            new InfinispanWriteJob(configuration).runJob(iterator, ctx)
         }
         rdd.sparkContext.runJob(rdd, processor)
      }

      private class InfinispanWriteJob(val configuration: ConnectorConfiguration) extends Serializable {
         private def getCacheManager: RemoteCacheManager = RemoteCacheManagerBuilder.create(configuration)

         def runJob(iterator: Iterator[(K, V)], ctx: TaskContext): Unit = {
            val remoteCacheManager = getCacheManager
            ctx.addTaskCompletionListener { f => remoteCacheManager.stop() }
            val cache = getCache[K, V](configuration, remoteCacheManager)
            val batchSize = configuration.getWriteBatchSize
            iterator.grouped(batchSize).foreach(kv => cache.putAll(mapAsJavaMap(kv.toMap)))
         }
      }

   }


}
