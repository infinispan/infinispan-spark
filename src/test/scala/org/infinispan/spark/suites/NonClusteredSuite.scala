package org.infinispan.spark.suites

import org.infinispan.spark.test._
import org.scalatest.DoNotDiscover

@DoNotDiscover
class NonClusteredSuite extends RDDRetrievalTest with WordCache with Spark with SingleServer {
   override protected def getNumEntries: Int = 100
}