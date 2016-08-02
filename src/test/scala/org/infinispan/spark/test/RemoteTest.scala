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

   val node: SingleNode

   override def getServerPort = node.getServerPort

   override protected def beforeAll(): Unit = {
      node.start()
      withFilters().foreach(node.addFilter)
      node.createCache(getCacheName, getCacheConfig)
      getRemoteCache.clear()
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      withFilters().foreach(node.removeFilter)
      super.afterAll()
   }
}


@DoNotDiscover
trait SingleStandardServer extends SingleServer {
   this: Suite =>

   override val node = SingleStandardNode
}

@DoNotDiscover
trait SingleSecureServer extends SingleServer {
   this: Suite =>

   val KeyStore =  getClass.getResource("/keystore_client.jks").getFile
   val TrustStore =  getClass.getResource("/truststore_client.jks").getFile
   val StorePassword = "secret".toCharArray

   override val node = SingleSecureNode

   override protected lazy val remoteCacheManager = new RemoteCacheManager(
      new ConfigurationBuilder().addServer().host("localhost").port(getServerPort)
              .security().ssl().enable()
              .keyStoreFileName(KeyStore)
              .keyStorePassword(StorePassword)
              .trustStoreFileName(TrustStore)
              .trustStorePassword(StorePassword)
              .build
   )

   override def getConfiguration = {
      val config = super.getConfiguration
      config.put("infinispan.client.hotrod.use_ssl", "true")
      config.put("infinispan.client.hotrod.key_store_file_name", KeyStore)
      config.put("infinispan.client.hotrod.trust_store_file_name", TrustStore)
      config.put("infinispan.client.hotrod.key_store_password", "secret")
      config.put("infinispan.client.hotrod.trust_store_password", "secret")
      config
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


