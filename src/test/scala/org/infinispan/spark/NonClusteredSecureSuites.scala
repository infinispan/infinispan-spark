package org.infinispan.spark

import org.infinispan.spark.suites.{JavaStreamApiSecureSuite, NonClusteredSecureSuite, WriteSecureSuite}
import org.infinispan.spark.test.SingleSecureNode
import org.scalatest.{BeforeAndAfterAll, Suites}

/**
  * Aggregates all suites that require a single non clustered server with SSL/Security enabled
  */
class NonClusteredSecureSuites extends Suites(new NonClusteredSecureSuite, new WriteSecureSuite, new JavaStreamApiSecureSuite) with BeforeAndAfterAll {

   override protected def beforeAll(): Unit = {
      SingleSecureNode.start()
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      SingleSecureNode.shutDown()
      super.afterAll()
   }
}
