package org.infinispan.spark.test

import org.jboss.dmr.scala.ModelNode

import scala.concurrent.duration._
import scala.language.postfixOps


object ClusterSample {
   def main(args: Array[String]) {

      val cacheConfig = ModelNode(
         "expiration" -> ModelNode(
            "EXPIRATION" -> ModelNode(
               "interval" -> 10000,
               "lifespan" -> 10,
               "max-idle" -> 10
            )
         ),
         "compatibility" -> ModelNode(
            "COMPATIBILITY" -> ModelNode(
               "enabled" -> true,
               "marshaller" -> "org.infinispan.commons.marshall.jboss.GenericJBossMarshaller"
            )
         ),
         "indexing" -> "ALL",
         "indexing-properties" -> ModelNode(
            "default.directory_provider" -> "infinispan",
            "default.metadata_cachename" -> "indexMetadata",
            "default.data_cachename" -> "indexData"
         ), "file-store" -> ModelNode(
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

      val cluster = new Cluster(size = 3, location = s"${System.getProperty("user.home")}/infinispan-server-8.1.0.Alpha2")

      cluster.startAndWait(20 seconds)

      val cache = cluster.obtainCache("my-test-cache", CacheType.DISTRIBUTED, Some(cacheConfig))

      assert(cache.size() == 0)

      cluster.shutDown()
   }

}
