package org.infinispan.spark.test

import org.scalatest.{BeforeAndAfterEach, Suite}

/**
  * Trait to be mixed-in by tests which shut down some servers or need to restore cluster before each test for any other reason.
  *
  * @author vjuranek
  *
  */
trait FailOver extends BeforeAndAfterEach {
   this: Suite =>

   override def beforeEach() {
      Cluster.restore()
      super.beforeEach()
   }

   override def afterEach() {
      Cluster.restore()
      super.afterEach()
   }

}
