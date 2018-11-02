package org.infinispan.spark.suites

import org.infinispan.commons.dataconversion.MediaType
import org.infinispan.commons.marshall.UTF8StringMarshaller
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.test.{RunnersCache, SingleStandardServer, Spark}
import org.jboss.dmr.scala.ModelNode
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

@DoNotDiscover
class DataFormatSuite extends FunSuite with RunnersCache with Spark with SingleStandardServer with Matchers {

   override protected def getNumEntries: Int = 10

   override def getCacheConfig: Option[ModelNode] = Some(
      ModelNode("encoding" -> ModelNode(
         "key" -> ModelNode("media-type" -> "application/x-jboss-marshalling"),
         "value" -> ModelNode("media-type" -> "application/x-jboss-marshalling"))
      )
   )

   override def getConfiguration: ConnectorConfiguration = {
      val config = super.getConfiguration
      config.setValueMediaType(MediaType.APPLICATION_JSON_TYPE)
         .setValueMarshaller(classOf[UTF8StringMarshaller])
   }

   test("read data as JSON") {
      val rdd = createInfinispanRDD[Int, String]
      val json = ujson.read(rdd.values.first)

      json("_type").str shouldBe classOf[Runner].getName
      json("name").str should startWith("Runner")
      json("age").num.toInt should be > 0
   }

}
