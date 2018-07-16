package org.infinispan.spark.test

import java.io.Serializable

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.filter.{AbstractKeyValueFilterConverter, KeyValueFilterConverterFactory, NamedFactory}
import org.infinispan.metadata.Metadata
import org.infinispan.spark.domain._
import org.jboss.dmr.scala.ModelNode

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Standalone example of interacting with the server: defining custom caches configurations,
 * adding filters, starting and stopping cluster.
 *
 */
object ClusterSample {
   def main(args: Array[String]) {

      if(args.isEmpty) {
         println("Usage: ClusterSample /path/to/server")
         sys.exit(1)
      }

      val serverLocation = args(0)

      @NamedFactory(name = "sample-filter-factory")
      class SampleFilterFactory extends KeyValueFilterConverterFactory[Integer, Runner, String] with Serializable {
         override def getFilterConverter = new SampleFilter

         class SampleFilter extends AbstractKeyValueFilterConverter[Integer, Runner, String] with Serializable {
            override def filterAndConvert(k: Integer, v: Runner, metadata: Metadata): String = v.getName
         }

      }

      val cacheConfig = ModelNode(
         "expiration" -> ModelNode(
            "EXPIRATION" -> ModelNode(
               "interval" -> 10000,
               "lifespan" -> 20000,
               "max-idle" -> 30000
            )
         ),
         "compatibility" -> ModelNode(
            "COMPATIBILITY" -> ModelNode(
               "enabled" -> false
            )
         ),
          "file-store" -> ModelNode(
            "FILE_STORE" -> ModelNode(
               "fetch-state" -> false,
               "passivation" -> false,
               "preload" -> true,
               "purge" -> false,
               "write-behind" -> ModelNode(
                  "WRITE_BEHIND" -> ModelNode(
                     "flush-lock-timeout" -> 2,
                     "modification-queue-size" -> 2048,
                     "shutdown-timeout" -> 20000,
                     "thread-pool-size" -> 1
                  )
               )
            )
         ))

      val cluster = new Cluster(size = 3, location = serverLocation)

      cluster.addEntities(
         new EntityDef(
            Seq(classOf[Person], classOf[Runner], classOf[Address]),
            Seq("org.infinispan.commons", "org.infinispan.protostream"),
            "my-entities.jar")
      )

      cluster.startAndWait(20 seconds)

      val filterDef = new FilterDef(
         classOf[SampleFilterFactory],
         Seq(TestEntities.moduleName),
         Seq(classOf[SampleFilterFactory#SampleFilter])
      )

      cluster addFilter filterDef

      cluster.createCache[Int,Runner]("my-test-cache", CacheType.DISTRIBUTED, Some(cacheConfig))

      val builder = new ConfigurationBuilder().addServer().host("localhost").port(cluster.getFirstServer.getHotRodPort).build

      val cache: RemoteCache[Int,Runner] = new RemoteCacheManager(builder).getCache("my-test-cache")

      cache.put(1, new Runner("Runner1", true, 3600, 34))
      assert(cache.size() == 1)

      val converted = cache.retrieveEntries("sample-filter-factory", 10).next().getValue
      assert(converted == "Runner1")

      cluster removeFilter filterDef

      cluster.shutDown()
   }

}
