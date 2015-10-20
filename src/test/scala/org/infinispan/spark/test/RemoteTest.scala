package org.infinispan.spark.test

import java.util.Properties

import org.infinispan.client.hotrod.RemoteCache
import org.scalatest.{DoNotDiscover, BeforeAndAfterAll, Suite}

/**
 * Trait to be mixed-in by tests that require a reference to a RemoteCache
 *
 * @author gustavonalle
 */
sealed trait RemoteTest {
   def getCacheName: String

   def getTargetCache[K, V]: RemoteCache[K, V]

   def getServerPort: Int

   def getConfiguration = {
      val properties = new Properties()
      val port = getServerPort
      properties.put("infinispan.client.hotrod.server_list", Seq("localhost", port).mkString(":"))
      properties.put("infinispan.rdd.cacheName", getTargetCache.getName)
      properties
   }
}

/**
 * Traits to be mixed-in for a single server with a custom cache
 */
@DoNotDiscover
trait SingleServer extends RemoteTest with BeforeAndAfterAll {
   this: Suite =>

   private var remoteCache: RemoteCache[_, _] = _

   override def getTargetCache[K, V]: RemoteCache[K, V] = remoteCache.asInstanceOf[RemoteCache[K, V]]

   override def getServerPort = SingleNode.getServerPort

   override protected def beforeAll(): Unit = {
      SingleNode.start()
      remoteCache = SingleNode.getOrCreateCache(getCacheName)
      remoteCache.clear()
      super.beforeAll()
   }

}

trait MultipleServers extends RemoteTest with BeforeAndAfterAll {
   this: Suite =>

   def getCacheType: CacheType.Value

   override def getTargetCache[K, V] = Cluster.getOrCreateCache(getCacheName, getCacheType).asInstanceOf[RemoteCache[K, V]]

   override def getServerPort = Cluster.getFirstServerPort

   override protected def beforeAll(): Unit = {
      Cluster.start()
      super.beforeAll()
   }
}


