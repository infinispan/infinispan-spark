package org.infinispan.spark.test

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.rdd.RemoteCacheManagerBuilder
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Suite}

/**
 * Trait to be mixed-in by tests that require a reference to a RemoteCache
 *
 * @author gustavonalle
 */
sealed trait RemoteTest {

   protected def getRemoteCache[K, V]: RemoteCache[K, V] = remoteCacheManager.getCache(getCacheName)

   protected lazy val remoteCacheManager = RemoteCacheManagerBuilder.create(getConfiguration)

   def getCacheName: String = getClass.getName

   def getCacheConfig: Option[String] = None

   def getServerPort: Int

   def getConfiguration = {
      val config = new ConnectorConfiguration()
      val port = getServerPort
      config.setServerList(Seq("localhost", port).mkString(":"))
      config.setCacheName(getCacheName)
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
      node.createCache(getCacheName, getCacheConfig)
      getRemoteCache.clear()
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
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

   val KeyStore = getClass.getResource("/keystore_client.jks").getFile
   val TrustStore = getClass.getResource("/truststore_client.jks").getFile
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
      val configuration = super.getConfiguration
      configuration
        .addHotRodClientProperty("infinispan.client.hotrod.use_ssl", "true")
        .addHotRodClientProperty("infinispan.client.hotrod.key_store_file_name", KeyStore)
        .addHotRodClientProperty("infinispan.client.hotrod.trust_store_file_name", TrustStore)
        .addHotRodClientProperty("infinispan.client.hotrod.key_store_password", "secret")
        .addHotRodClientProperty("infinispan.client.hotrod.trust_store_password", "secret")

   }


}

trait MultipleServers extends RemoteTest with BeforeAndAfterAll {
   this: Suite =>

   def getCacheType: CacheType.Value

   override def getServerPort = Cluster.getFirstServerPort

   override protected def beforeAll(): Unit = {
      Cluster.start()
      Cluster.createCache(getCacheName, getCacheConfig)
      getRemoteCache.clear()
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      super.afterAll()
   }
}


