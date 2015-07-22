package org.infinispan.spark.suites

import org.infinispan.spark.test._
import org.scalatest.DoNotDiscover

@DoNotDiscover
class ReplicatedSuite extends RDDRetrievalTest with WordCache with Spark with MultipleServers {
   override protected def getNumEntries = 100

   override def getCacheType = CacheType.REPLICATED
}
