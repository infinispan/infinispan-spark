package org.infinispan.spark

import org.infinispan.spark.suites.{DistributedSuite, ReplicatedSuite, SQLSuite, StreamingSuite}
import org.infinispan.spark.test.Cluster
import org.scalatest.{BeforeAndAfterAll, Suites}

import scala.language.postfixOps

/**
 * Aggregates all suites that requires a running cluster.
 */
class ClusteredSuites extends Suites(new DistributedSuite, new ReplicatedSuite, new SQLSuite, new StreamingSuite) with BeforeAndAfterAll {

   override protected def beforeAll(): Unit = {
      Cluster.start()
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      Cluster.shutDown()
      super.afterAll()
   }
}
