package org.infinispan.spark.test

import java.io.Serializable

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.filter.{AbstractKeyValueFilterConverter, KeyValueFilterConverterFactory, NamedFactory}
import org.infinispan.metadata.Metadata
import org.infinispan.spark.domain._

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Standalone example of interacting with the server: defining custom caches configurations,
 * adding filters, starting and stopping cluster.
 *
 */
object ClusterSample {
   def main(args: Array[String]) {

      if (args.isEmpty) {
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
      val cacheConfig =
         """
           |{
           |    "distributed-cache":{
           |        "mode":"SYNC",
           |        "segments":2,
           |        "statistics":true,
           |        "expiration":{
           |            "max-idle":100000,
           |            "interval":5000,
           |            "lifespan":20000
           |        },
           |        "encoding":{
           |            "key":{
           |                "media-type":"application/x-java-object"
           |            },
           |            "value":{
           |                "media-type":"application/x-java-object"
           |            }
           |        },
           |        "file-store":{
           |            "path":"path",
           |            "relative-to":"jboss.server.temp.dir",
           |            "shared":false,
           |            "fetch-state":false,
           |            "preload":true,
           |            "purge":false,
           |            "write-behind":{
           |                "modification-queue-size":2048,
           |                "thread-pool-size":1
           |            }
           |        }
           |    }
           |}
           |""".stripMargin

      val cluster = new Cluster(size = 3, location = serverLocation, cacheContainer = "default")

      cluster.addEntities(
         new EntityDef(Seq(classOf[Person], classOf[Runner], classOf[Address]), "my-entities.jar")
      )

      val filterDef = new FilterDef(
         classOf[SampleFilterFactory],
         Seq(classOf[SampleFilterFactory#SampleFilter])
      )

      cluster addFilter filterDef

      cluster.startAndWait(20 seconds)

      cluster.createCache[Int, Runner]("my-test-cache", Some(cacheConfig))

      val builder = new ConfigurationBuilder().addServer().host("localhost").port(cluster.getFirstServer.getHotRodPort).build

      val manager = new RemoteCacheManager(builder)

      val cache: RemoteCache[Int, Runner] = manager.getCache("my-test-cache")

      cache.put(1, new Runner("Runner1", true, 3600, 34))
      assert(cache.size() == 1)

      val converted = cache.retrieveEntries("sample-filter-factory", 10).next().getValue
      assert(converted == "Runner1")

      cluster removeFilter filterDef

      manager.stop();

      cluster.shutDown()
   }

}
