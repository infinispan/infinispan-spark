package org.infinispan.spark.test

import org.infinispan.spark.domain.Runner
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.util.Random

/**
 * Trait to be mixed-in by tests requiring a cache populated with [[org.infinispan.spark.domain.Runner]] objects.
 *
 * @author gustavonalle
 */
trait RunnersCache extends BeforeAndAfterAll {
   this: Suite with RemoteTest =>

   protected def getNumEntries: Int

   override protected def beforeAll(): Unit = {

      val random = new Random(System.currentTimeMillis())
      val MinFinishTime = 3600
      val MaxFinishTime = 4500
      val MinAge = 15
      val MaxAge = 60
      (1 to getNumEntries).par.foreach { i =>
         val name = "Runner " + i
         val finished = if (i % 2 == 0) true else false
         val finishTime = random.nextInt((MaxFinishTime - MinFinishTime) + 1) + MinFinishTime
         val age = Integer.valueOf(i * (MaxAge - MinAge) / getNumEntries + MinAge)
         val runner = new Runner(name, finished, if(finished) finishTime else 0, age)
         getRemoteCache.put(i, runner)
      }
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      super.afterAll()
   }
}
