package org.infinispan.spark

import org.infinispan.spark.suites._
import org.infinispan.spark.test.Cluster
import org.scalatest.{BeforeAndAfterAll, Suites}


/**
 * Aggregates all suites that requires a running cluster.
 */
class ClusteredSuites extends Suites(new DistributedSuite, new ReplicatedSuite, new SQLSuite, new StreamingSuite,
   new FilterByQueryProtoAnnotationSuite, new CustomFilterRDDSuite, new FilterByQueryProtoSuite) with BeforeAndAfterAll {

   override protected def beforeAll(): Unit = {
      Cluster.start()
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      Cluster.shutDown()
      super.afterAll()
   }
}
