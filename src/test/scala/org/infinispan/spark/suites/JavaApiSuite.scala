package org.infinispan.spark.suites

import org.infinispan.spark.JavaApiTest
import org.infinispan.spark.test.SampleFilters.AgeFilterFactory
import org.infinispan.spark.test.{FilterDef, JavaSpark, SingleStandardServer, TestEntities}
import org.scalatest._

@DoNotDiscover
class JavaApiSuite extends FunSuite with JavaSpark with SingleStandardServer with Matchers {

   lazy val javaTest: JavaApiTest = new JavaApiTest(jsc, sparkSession, getRemoteCache, getConfiguration)

   override def withFilters() = List(
      new FilterDef(
         factoryClass = classOf[AgeFilterFactory],
         classes = Seq(classOf[AgeFilterFactory#AgeFilter]),
         moduleDeps = Seq(TestEntities.moduleName, "org.scala")
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
