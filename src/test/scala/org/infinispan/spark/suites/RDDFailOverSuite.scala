package org.infinispan.spark.suites

import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.test.TestingUtil._
import org.infinispan.spark.test._
import org.scalatest._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

@DoNotDiscover
class RDDFailOverSuite extends FunSuite with Matchers with WordCache with Spark with MultipleServers with FailOver {

   override protected def getNumEntries: Int = 100

   override def getCacheType = CacheType.DISTRIBUTED

   override def getConfiguration = {
      val properties = super.getConfiguration
      properties.put("infinispan.client.hotrod.server_list", "127.0.0.1:11222")
      properties.put("infinispan.rdd.write_batch_size", int2Integer(5))
      properties
   }

   test("RDD read failover") {
      val infinispanRDD = createInfinispanRDD[Int, String]

      val ispnIter = infinispanRDD.toLocalIterator
      var count = 0
      for (i <- 1 to getNumEntries/Cluster.getClusterSize) {
         ispnIter.next()
         count += 1
      }

      Cluster.failServer(0)

      while (ispnIter.hasNext) {
         ispnIter.next()
         count += 1
      }

      count shouldBe getNumEntries
   }

   test("RDD write failover") {
      val cache = getRemoteCache.asInstanceOf[RemoteCache[Int, Runner]]
      cache.clear()

      val range1 = 0 to 999
      val entities1 = (for (num <- range1) yield new Runner(s"name$num", true, num * 10, 20)).toSeq
      val rdd = sc.parallelize(range1.zip(entities1))

      val writeRDD = Future(rdd.writeToInfinispan(getConfiguration))
      waitForCondition ({ () =>
         cache.size() > 0 //make sure we are already writing into the cache
      }, 1000, 1)
      Cluster.failServer(0)
      Await.ready(writeRDD, 30 second)

      cache.size() shouldBe 1000
      cache.get(350).getName shouldBe "name350"
   }
}


