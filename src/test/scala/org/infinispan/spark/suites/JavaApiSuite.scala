package org.infinispan.spark.suites

import org.infinispan.spark.JavaApiTest
import org.infinispan.spark.domain._
import org.infinispan.spark.test.SampleFilters.AgeFilterFactory
import org.infinispan.spark.test.{FilterDef, JavaSpark, SingleStandardServer}
import org.scalatest._

@DoNotDiscover
class JavaApiSuite extends FunSuite with JavaSpark with SingleStandardServer with Matchers {

   lazy val javaTest: JavaApiTest = new JavaApiTest(jsc, sparkSession, getRemoteCache, getConfiguration)


   override def withFilters() = List(
      new FilterDef(
         classOf[AgeFilterFactory],
         classOf[AgeFilterFactory#AgeFilter],
         classOf[Person],
         classOf[Address]
      )
   )

   test("RDD Read from Java") {
      javaTest.testRDDRead()
   }

   test("RDD Write from Java") {
      javaTest.testRDDWrite()
   }

   test("SQL Read from Java") {
      javaTest.testSQL()
   }

   test("Filter by deployed filter") {
      javaTest.testFilterByDeployedFilter()
   }
}
