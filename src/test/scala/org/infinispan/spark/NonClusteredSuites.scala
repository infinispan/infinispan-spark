package org.infinispan.spark

import org.infinispan.spark.suites._
import org.infinispan.spark.test.SingleStandardNode
import org.scalatest.{BeforeAndAfterAll, Suites}

/**
 * Aggregates all suites that require a single non clustered server.
 */
class NonClusteredSuites extends Suites(new NonClusteredSuite, new WriteSuite, new JavaApiSuite, new JavaStreamApiSuite, new HiveContextSuite) with BeforeAndAfterAll {

   override protected def beforeAll(): Unit = {
      SingleStandardNode.start()
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      SingleStandardNode.shutDown()
      super.afterAll()
   }
}



