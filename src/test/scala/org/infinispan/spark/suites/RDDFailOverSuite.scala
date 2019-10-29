package org.infinispan.spark.suites

import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.spark._
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.test.TestingUtil._
import org.infinispan.spark.test._
import org.scalatest._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

@DoNotDiscover
class RDDFailOverSuite extends FunSuite with Matchers with Spark with MultipleServers with FailOver {

   val NumEntries = 10000

   override def getConfiguration: ConnectorConfiguration = {
      super.getConfiguration.setServerList("127.0.0.1:11222;127.0.0.1:12222")
        .setWriteBatchSize(5)
   }

   test("RDD read failover") {
      val cache = getRemoteCache.asInstanceOf[RemoteCache[Int, Runner]]
      cache.clear()
      (0 until NumEntries).foreach(id => cache.put(id, new Runner(s"name$id", true, id * 10, 20)))

      val infinispanRDD = createInfinispanRDD[Int, String]

      val ispnIter = infinispanRDD.toLocalIterator
      var count = 0
      for (_ <- 1 to NumEntries / Cluster.getClusterSize) {
         ispnIter.next()
         count += 1
      }

      Cluster.failServer(0)

      while (ispnIter.hasNext) {
         ispnIter.next()
         count += 1
      }

      count shouldBe NumEntries
   }

   ignore("RDD write failover (Re-test with 10.1.0.Final)") {
      val cache = getRemoteCache.asInstanceOf[RemoteCache[Int, Runner]]
      cache.clear()

      val range1 = 1 to NumEntries
      val entities1 = for (num <- range1) yield new Runner(s"name$num", true, num * 10, 20)
      val rdd = sc.parallelize(range1.zip(entities1))

      val writeRDD = Future(rdd.writeToInfinispan(getConfiguration))
      waitForCondition({ () =>
         cache.size() > 0 //make sure we are already writing into the cache
      }, 2 seconds)
      Cluster.failServer(0)
      Await.ready(writeRDD, 30 second)

      cache.size() shouldBe NumEntries
      cache.get(350).getName shouldBe "name350"
   }
}


