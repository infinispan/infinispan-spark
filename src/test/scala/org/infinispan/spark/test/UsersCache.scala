package org.infinispan.spark.test

import org.infinispan.spark.domain.User
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Trait to be mixed-in by tests requiring a cache populated with [[org.infinispan.spark.domain.User]] objects (scala objects)
  */
trait UsersCache extends BeforeAndAfterAll {
   this: Suite with RemoteTest =>

   protected def getNumEntries: Int

   override protected def beforeAll(): Unit = {
      val MinAge = 15
      val MaxAge = 60
      (1 to getNumEntries).par.foreach { i =>
         val name = "User " + i
         val age = Integer.valueOf(i * (MaxAge - MinAge) / getNumEntries + MinAge)
         val user = new User(name, age)
         user.setName(name)
         user.setAge(age)
         getRemoteCache.put(i, user)
      }
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      super.afterAll()
   }
}
