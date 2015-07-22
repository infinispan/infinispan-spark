package org.infinispan.spark

import org.infinispan.spark.suites._
import org.infinispan.spark.test.SingleNode
import org.scalatest.{BeforeAndAfterAll, Suites}

/**
 * Aggregates all suites that require a single non clustered server.
 */
class NonClusteredSuites extends Suites(new NonClusteredSuite, new WriteSuite, new JavaApiSuite, new JavaStreamApiSuite) with BeforeAndAfterAll {

   override protected def beforeAll(): Unit = {
      SingleNode.start()
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      SingleNode.shutDown()
      super.afterAll()
   }
}



