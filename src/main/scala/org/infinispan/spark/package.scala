package org.infinispan

import java.net.InetSocketAddress

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.infinispan.client.hotrod.{CacheTopologyInfo, DataFormat, RemoteCache, RemoteCacheManager}
import org.infinispan.commons.dataconversion.MediaType
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.rdd.RemoteCacheManagerBuilder

import scala.collection.JavaConverters._

package object spark {

   def getCacheTopology(cacheTopology: CacheTopologyInfo): String = {
      val segmentsPerServer = cacheTopology.getSegmentsPerServer
      segmentsPerServer.keySet.asScala.map {
         case i: InetSocketAddress => s"${i.getHostString}:${i.getPort}"
      }.mkString(";")
   }

   def decorateWithFormat(config: ConnectorConfiguration, cache: RemoteCache[_, _]): RemoteCache[_, _] = {
      if (!config.hasCustomFormat) cache else {
         val dataFormat = DataFormat.builder()
         Option(config.getKeyMediaType).map(MediaType.fromString).foreach(dataFormat.keyType)
         Option(config.getValueMediaType).map(MediaType.fromString).foreach(dataFormat.valueType)
         Option(config.getKeyMarshaller).map(_.newInstance).foreach(dataFormat.keyMarshaller)
         Option(config.getValueMarshaller).map(_.newInstance).foreach(dataFormat.valueMarshaller)
         cache.withDataFormat(dataFormat.build())
      }
   }

   def getCache[K, V](config: ConnectorConfiguration, rcm: RemoteCacheManager): RemoteCache[K, V] = {
      val cacheName = config.getCacheName
      val remoteCache = Option(cacheName).map(name => rcm.getCache[K, V](name)).getOrElse(rcm.getCache[K, V])
      decorateWithFormat(config, remoteCache).asInstanceOf[RemoteCache[K, V]]
   }

   implicit class RDDExtensions[K, V](rdd: RDD[(K, V)]) extends Serializable {

      def writeToInfinispan(configuration: ConnectorConfiguration): Unit = {
         val processor = (ctx: TaskContext, iterator: Iterator[(K, V)]) => {
            val remoteCacheManager = RemoteCacheManagerBuilder.create(configuration)
            val cache = getCache[K, V](configuration, remoteCacheManager)
            configuration.setServerList(getCacheTopology(cache.getCacheTopologyInfo))
            ctx.addTaskCompletionListener[Unit](_ => remoteCacheManager.stop())
            new InfinispanWriteJob(configuration).runJob(iterator, ctx)
         }
         rdd.sparkContext.runJob(rdd, processor)
      }

      private class InfinispanWriteJob(val configuration: ConnectorConfiguration) extends Serializable {
         private def getCacheManager: RemoteCacheManager = RemoteCacheManagerBuilder.create(configuration)

         def runJob(iterator: Iterator[(K, V)], ctx: TaskContext): Unit = {
            val remoteCacheManager = getCacheManager
            ctx.addTaskCompletionListener[Unit](_ => remoteCacheManager.stop())
            val cache = getCache[K, V](configuration, remoteCacheManager)
            val batchSize = configuration.getWriteBatchSize
            iterator.grouped(batchSize).foreach(kv => cache.putAll(mapAsJavaMap(kv.toMap)))
         }
      }

   }


}
