package org.infinispan

import java.util.Properties

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark.rdd.InfinispanRDD

import scala.collection.JavaConversions._

package object spark {

   implicit class EnhancedProperties(props: Properties) {
      def readWithDefault[T](key: String)(default: T) = read[T](key).getOrElse(default)

      def read[T](key: String): Option[T] = Option(props.get(key)).map(_.asInstanceOf[T])
   }

   implicit class RDDExtensions[K, V](rdd: RDD[(K, V)]) extends Serializable {

      def writeToInfinispan(configuration: Properties): Unit = {
         val processor = (ctx: TaskContext, iterator: Iterator[(K, V)]) => {
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
            val cacheName = configuration.getProperty(InfinispanRDD.CacheName)
            val cache = Option(cacheName).map(name => remoteCacheManager.getCache(name)).getOrElse(remoteCacheManager.getCache).asInstanceOf[RemoteCache[K, V]]
            val batchSize = configuration.readWithDefault[Int](InfinispanRDD.WriteBatchSize)(InfinispanRDD.DefaultWriteBatchSize)
            iterator.grouped(batchSize).foreach(kv => cache.putAll(mapAsJavaMap(kv.toMap)))
         }
      }

   }




}
