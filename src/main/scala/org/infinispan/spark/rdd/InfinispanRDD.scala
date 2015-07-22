package org.infinispan.spark.rdd

import java.net.InetSocketAddress
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.spark._

import scala.collection.JavaConversions._

/**
 * @author gustavonalle
 */
class InfinispanRDD[K, V](@transient val sc: SparkContext,
                          val configuration: Properties,
                          @transient val splitter: Splitter = new PerServerSplitter)
        extends RDD[(K, V)](sc, Nil) {

   override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
      logInfo(s"Computing partition $split")
      val infinispanPartition = split.asInstanceOf[InfinispanPartition]
      val config = infinispanPartition.properties
      val cacheName = config.getProperty(InfinispanRDD.CacheName)
      val batch = config.readWithDefault[Integer](InfinispanRDD.ReadBatchSize)(default = InfinispanRDD.DefaultReadBatchSize)
      val address = infinispanPartition.location.address.asInstanceOf[InetSocketAddress]
      val builder = new ConfigurationBuilder().withProperties(configuration)
              .addServer().host(address.getHostString).port(address.getPort)
              .balancingStrategy(new PreferredServerBalancingStrategy(address))
      val remoteCacheManager = new RemoteCacheManager(builder.build())
      val cache = Option(cacheName).map(name => remoteCacheManager.getCache(name)).getOrElse(remoteCacheManager.getCache)
      val segmentFilter = infinispanPartition.segments.map(setAsJavaSet).orNull
      val filterFactory = config.read[String](InfinispanRDD.FilterFactory)
      val closeableIterator = cache.retrieveEntries(filterFactory.getOrElse(null.asInstanceOf[String]), segmentFilter, batch)
      context.addTaskCompletionListener(t => {
         closeableIterator.close()
         remoteCacheManager.stop()
      })
      new InfinispanIterator(closeableIterator, context)
   }

   override protected def getPreferredLocations(split: Partition): Seq[String] =
      Seq(split.asInstanceOf[InfinispanPartition].location.address.asInstanceOf[InetSocketAddress].getHostString)

   override protected def getPartitions: Array[Partition] = {
      val remoteCacheManager = new RemoteCacheManager(new ConfigurationBuilder().withProperties(configuration).pingOnStartup(true).build())
      val optCacheName = Option(configuration.getProperty(InfinispanRDD.CacheName))
      val cache = optCacheName.map(name => remoteCacheManager.getCache(name)).getOrElse(remoteCacheManager.getCache)
      val segmentsByServer = cache.getCacheTopologyInfo
      splitter.split(segmentsByServer, configuration)
   }
}

object InfinispanRDD {
   val DefaultReadBatchSize = 10000
   val DefaultWriteBatchSize = 500
   val DefaultPartitionsPerServer = 2

   val CacheName = "infinispan.rdd.cacheName"
   val ReadBatchSize = "infinispan.rdd.read_batch_size"
   val WriteBatchSize = "infinispan.rdd.write_batch_size"
   val PartitionsPerServer = "infinispan.rdd.number_server_partitions"
   val FilterFactory = "infinispan.rdd.filter_factory"
}
