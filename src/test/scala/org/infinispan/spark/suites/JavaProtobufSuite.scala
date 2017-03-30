package org.infinispan.spark.suites

import org.infinispan.spark.JavaProtobufTest
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.test.{JavaSpark, RunnersCache, SingleStandardServer}
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

@DoNotDiscover
class JavaProtobufSuite extends FunSuite with RunnersCache with JavaSpark with SingleStandardServer with Matchers {

   override protected def getNumEntries: Int = 500

   override def getConfiguration: ConnectorConfiguration = {
      super.getConfiguration
        .setAutoRegisterProto()
        .addProtoAnnotatedClass(classOf[Runner])
   }

   lazy val javaTest = new JavaProtobufTest(sparkSession, getRemoteCache, getConfiguration)

   test("read data using the DataFrame API") {
      javaTest.testDataFrameApi()
   }

   test("read using SQL and projections") {
      javaTest.testSQL()
   }

}
