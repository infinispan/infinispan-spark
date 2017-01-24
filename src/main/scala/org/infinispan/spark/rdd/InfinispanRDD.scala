package org.infinispan.spark.rdd

import java.net.InetSocketAddress
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.impl.ConfigurationProperties.SERVER_LIST
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.query.dsl.Query
import org.infinispan.query.dsl.impl.BaseQuery
import org.infinispan.spark._

import scala.collection.JavaConversions._

/**
 * @author gustavonalle
 */
class InfinispanRDD[K, V](@transient val sc: SparkContext,
                          val configuration: Properties,
                          @transient val splitter: Splitter = new PerServerSplitter)
        extends RDD[(K, V)](sc, Nil) {

   private[rdd] def createBuilder(preferredAddress: InetSocketAddress, properties: Properties) =
      new ConfigurationBuilder().withProperties(properties).balancingStrategy(new PreferredServerBalancingStrategy(preferredAddress))

   @transient lazy val remoteCache: RemoteCache[K, V] = {
      val config = new ConfigurationBuilder().withProperties(configuration).build()
      val remoteCacheManager = new RemoteCacheManager(config)
      sc.addSparkListener(new SparkListener {
         override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = remoteCacheManager.stop()
      })
      getCache[K, V](configuration, remoteCacheManager)
   }

   def compute[R](split: Partition, context: TaskContext,
               cacheManagerProvider: (InetSocketAddress, Properties) => RemoteCacheManager,
               filterFactory: String, factoryParams: Array[AnyRef]): Iterator[(K, R)] = {

      logInfo(s"Computing partition $split")
      val infinispanPartition = split.asInstanceOf[InfinispanPartition]
      val config = infinispanPartition.properties
      val batch = config.readWithDefault[Integer](InfinispanRDD.ReadBatchSize)(default = InfinispanRDD.DefaultReadBatchSize)
      val address = infinispanPartition.location.address.asInstanceOf[InetSocketAddress]
      val remoteCacheManager = cacheManagerProvider(address, config)
      val cache = getCache(config, remoteCacheManager)
      val segmentFilter = infinispanPartition.segments.map(setAsJavaSet).orNull
      val closeableIterator = cache.retrieveEntries(filterFactory, factoryParams, segmentFilter, batch)

      context.addTaskCompletionListener(t => {
         closeableIterator.close()
         remoteCacheManager.stop()
      })
      new InfinispanIterator(closeableIterator, context)
   }

   override def count() = remoteCache.size()

   override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
      compute(split, context, (a,p) => new RemoteCacheManager(createBuilder(a, p).build()), null, null)
   }

   def filterByQuery[R](q: Query, c: Class[_]*) = {
      new FilteredInfinispanRDD[K, V, R](this, Some(FilterQuery(q, q.asInstanceOf[BaseQuery].getQueryString, q.asInstanceOf[BaseQuery].getParameters, c: _*)), None)
   }

   def filterByCustom[R](filterFactory: String, params: AnyRef*): RDD[(K,R)] =
      new FilteredInfinispanRDD[K, V, R](this, None, Some(filterFactory), params:_*)

   override protected def getPreferredLocations(split: Partition): Seq[String] =
      Seq(split.asInstanceOf[InfinispanPartition].location.address.asInstanceOf[InetSocketAddress].getHostString)

   override def getPartitions: Array[Partition] = {
      val segmentsByServer = remoteCache.getCacheTopologyInfo
      configuration.put(SERVER_LIST, getCacheTopology(segmentsByServer))
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
   val ProtoFiles = "infinispan.rdd.query.proto.protofiles"
   val Marshallers = "infinispan.rdd.query.proto.marshallers"
}
