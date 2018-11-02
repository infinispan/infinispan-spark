package org.infinispan.spark.rdd

import java.net.InetSocketAddress
import java.util.Properties
import java.util.function.Supplier

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.{FailoverRequestBalancingStrategy, RemoteCache, RemoteCacheManager}
import org.infinispan.query.dsl.Query
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.{CacheAdmin, _}

/**
  * @author gustavonalle
  */
class InfinispanRDD[K, V](@transient val sc: SparkContext,
                          val configuration: ConnectorConfiguration,
                          @transient val splitter: Splitter = new PerServerSplitter)
   extends RDD[(K, V)](sc, Nil) with CacheManagementAware {

   private[rdd] def createBuilder(preferredAddress: InetSocketAddress, properties: Properties) = {
      val balancingFactory = new Supplier[FailoverRequestBalancingStrategy] {
         override def get(): FailoverRequestBalancingStrategy = new PreferredServerBalancingStrategy(preferredAddress)
      }
      new ConfigurationBuilder().withProperties(properties).balancingStrategy(balancingFactory)
   }

   @transient lazy val remoteCacheManager: RemoteCacheManager = RemoteCacheManagerBuilder.create(configuration)

   @transient lazy val _cacheAdmin = new CacheAdmin(remoteCacheManager)

   @transient lazy val remoteCache: RemoteCache[_, _] = {
      val remoteCacheManager = RemoteCacheManagerBuilder.create(configuration)
      sc.addSparkListener(new SparkListener {
         override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = remoteCacheManager.stop()
      })
      val cacheName = configuration.getCacheName
      val cacheCfg = configuration.getAutoCreateCacheFromConfig
      val cacheTemplate = configuration.getAutoCreateCacheFromTemplate
      if (cacheName != null && !_cacheAdmin.exists(cacheName)) {
         if (cacheCfg.isEmpty && cacheTemplate.isEmpty) throw new NonExistentCacheException
         if (cacheCfg.nonEmpty) {
            _cacheAdmin.createFromConfig(cacheName, cacheCfg)
         } else if (cacheTemplate.nonEmpty) {
            _cacheAdmin.createFromTemplate(cacheName, cacheTemplate)
         }
      }
      getCache[K, V](configuration, remoteCacheManager)
   }

   def compute[R](split: Partition, context: TaskContext,
                  cacheManagerProvider: (InetSocketAddress, ConnectorConfiguration) => RemoteCacheManager,
                  filterFactory: String, factoryParams: Array[AnyRef]): Iterator[(K, R)] = {

      logInfo(s"Computing partition $split")
      val infinispanPartition = split.asInstanceOf[InfinispanPartition]
      val config = infinispanPartition.properties
      val batch = config.getReadBatchSize
      val address = infinispanPartition.location.address.asInstanceOf[InetSocketAddress]
      val remoteCacheManager = cacheManagerProvider(address, config)
      val cache = getCache(config, remoteCacheManager)
      val segmentFilter = infinispanPartition.segments
      val closeableIterator = cache.retrieveEntries(filterFactory, factoryParams, segmentFilter, batch)

      context.addTaskCompletionListener(t => {
         closeableIterator.close()
         remoteCacheManager.stop()
      })
      new InfinispanIterator(closeableIterator, context)
   }

   override def count() = remoteCache.size()

   override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
      compute(split, context, (a, p) => RemoteCacheManagerBuilder.create(p, a), null, null)
   }

   def filterByQuery[R](q: Query) = new FilteredQueryInfinispanRDD[K, V, R](this, QueryObjectFilter(q))

   def filterByQuery[R](q: String) = new FilteredQueryInfinispanRDD[K, V, R](this, StringQueryFilter(q))

   def filterByCustom[R](filterFactory: String, params: AnyRef*): RDD[(K, R)] =
      new FilteredCustomInfinispanRDD[K, V, R](this, DeployedFilter(filterFactory, params: _*))

   override protected def getPreferredLocations(split: Partition): Seq[String] =
      Seq(split.asInstanceOf[InfinispanPartition].location.address.asInstanceOf[InetSocketAddress].getHostString)

   override def getPartitions: Array[Partition] = {
      val segmentsByServer = remoteCache.getCacheTopologyInfo
      configuration.setServerList(getCacheTopology(segmentsByServer))
      splitter.split(segmentsByServer, configuration)
   }

   override def cacheAdmin(): CacheAdmin = _cacheAdmin
}
