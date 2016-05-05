package org.infinispan.spark

import org.infinispan.spark.suites.{StreamingFailOverSuite, RDDFailOverSuite}
import org.infinispan.spark.test.Cluster
import org.scalatest.{BeforeAndAfterAll, Suites}

/**
  *  Aggregates all suites that perform server shutdown during the test.
  */
class FailOverSuites extends Suites(new RDDFailOverSuite, new StreamingFailOverSuite) with BeforeAndAfterAll {

   override protected def beforeAll(): Unit = {
      Cluster.start()
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      Cluster.shutDown()
      super.afterAll()
   }

}

