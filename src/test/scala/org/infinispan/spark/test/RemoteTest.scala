package org.infinispan.spark.test

import java.util.Properties

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.jboss.dmr.scala.ModelNode
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Suite}

/**
 * Trait to be mixed-in by tests that require a reference to a RemoteCache
 *
 * @author gustavonalle
 */
sealed trait RemoteTest {

   protected def getRemoteCache[K,V]: RemoteCache[K, V] = remoteCacheManager.getCache(getCacheName)

   protected lazy val remoteCacheManager = new RemoteCacheManager(
      new ConfigurationBuilder().addServer().host("localhost").port(getServerPort).build
   )

   def getCacheName: String = getClass.getName

   def getCacheConfig: Option[ModelNode] = None

   def getServerPort: Int

   def withFilters(): List[FilterDef] = List.empty

   def getConfiguration = {
      val properties = new Properties()
      val port = getServerPort
      properties.put("infinispan.client.hotrod.server_list", Seq("localhost", port).mkString(":"))
      properties.put("infinispan.rdd.cacheName", getRemoteCache.getName)
      properties
   }
}

/**
 * Traits to be mixed-in for a single server with a custom cache
 */
@DoNotDiscover
trait SingleServer extends RemoteTest with BeforeAndAfterAll {
   this: Suite =>

   override def getServerPort = SingleNode.getServerPort

   override protected def beforeAll(): Unit = {
      SingleNode.start()
      withFilters().foreach(SingleNode.addFilter)
      SingleNode.createCache(getCacheName, getCacheConfig)
      getRemoteCache.clear()
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      withFilters().foreach(SingleNode.removeFilter)
      super.afterAll()
   }
}

trait MultipleServers extends RemoteTest with BeforeAndAfterAll {
   this: Suite =>

   def getCacheType: CacheType.Value

   override def getServerPort = Cluster.getFirstServerPort

   override protected def beforeAll(): Unit = {
      Cluster.start()
      withFilters().foreach(Cluster.addFilter)
      Cluster.createCache(getCacheName, getCacheType, getCacheConfig)
      getRemoteCache.clear()
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      withFilters().foreach(Cluster.removeFilter)
      super.afterAll()
   }
}


