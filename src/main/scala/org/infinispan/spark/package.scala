package org.infinispan

import java.net.InetSocketAddress
import java.util.Properties

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.impl.ConfigurationProperties._
import org.infinispan.client.hotrod.{CacheTopologyInfo, RemoteCacheManager}
import org.infinispan.spark.rdd.InfinispanRDD

import scala.collection.JavaConversions._

package object spark {

   def getCacheTopology(cacheTopology: CacheTopologyInfo) = {
      val segmentsPerServer = cacheTopology.getSegmentsPerServer
      segmentsPerServer.keySet.map {
         case i: InetSocketAddress => s"${i.getHostString}:${i.getPort}"
      }.mkString(";")
   }

   def getCache[K, V](config: Properties, rcm: RemoteCacheManager) = {
      val cacheName = config.getProperty(InfinispanRDD.CacheName)
      Option(cacheName).map(name => rcm.getCache[K, V](name)).getOrElse(rcm.getCache[K, V])
   }

   implicit class EnhancedProperties(props: Properties) {
      def readWithDefault[T](key: String)(default: T) = read[T](key).getOrElse(default)

      def read[T](key: String): Option[T] = Option(props.get(key)).map(_.asInstanceOf[T])
   }

   implicit class RDDExtensions[K, V](rdd: RDD[(K, V)]) extends Serializable {

      def writeToInfinispan(configuration: Properties): Unit = {
         val processor = (ctx: TaskContext, iterator: Iterator[(K, V)]) => {
            val remoteCacheManager = new RemoteCacheManager(new ConfigurationBuilder().withProperties(configuration).build())
            val cache = getCache[K, V](configuration, remoteCacheManager)
            configuration.put(SERVER_LIST, getCacheTopology(cache.getCacheTopologyInfo))
            ctx.addTaskCompletionListener(ctx => remoteCacheManager.stop())
            new InfinispanWriteJob(configuration).runJob(iterator, ctx)
         }
         rdd.sparkContext.runJob(rdd, processor)
      }

      private class InfinispanWriteJob(val configuration: Properties) extends Serializable {
         private def getCacheManager: RemoteCacheManager = {
            val builder = new ConfigurationBuilder().withProperties(configuration)
            new RemoteCacheManager(builder.build())
         }

         def runJob(iterator: Iterator[(K, V)], ctx: TaskContext): Unit = {
            val remoteCacheManager = getCacheManager
            ctx.addTaskCompletionListener { f => remoteCacheManager.stop() }
            val cache = getCache[K, V](configuration, remoteCacheManager)
            val batchSize = configuration.readWithDefault[Int](InfinispanRDD.WriteBatchSize)(InfinispanRDD.DefaultWriteBatchSize)
            iterator.grouped(batchSize).foreach(kv => cache.putAll(mapAsJavaMap(kv.toMap)))
         }
      }

   }




}
