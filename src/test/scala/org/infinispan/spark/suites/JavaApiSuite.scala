package org.infinispan.spark.suites

import org.infinispan.spark.JavaApiTest
import org.infinispan.spark.test.{JavaSpark, SingleServer}
import org.scalatest._

@DoNotDiscover
class JavaApiSuite extends FunSuite with JavaSpark with SingleServer with Matchers {

   lazy val javaTest: JavaApiTest = new JavaApiTest(jsc, getRemoteCache, getConfiguration)

   test("RDD Read from Java") {
      javaTest.testRDDRead()
   }

   test("RDD Write from Java") {
      javaTest.testRDDWrite()
   }

   test("SQL Read from Java") {
      javaTest.testSQL()
   }

}
