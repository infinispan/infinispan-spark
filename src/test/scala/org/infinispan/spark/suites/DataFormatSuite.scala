package org.infinispan.spark.suites

import org.infinispan.commons.dataconversion.MediaType
import org.infinispan.commons.marshall.UTF8StringMarshaller
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.test.{RunnersCache, SingleStandardServer, Spark}
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

@DoNotDiscover
class DataFormatSuite extends FunSuite with RunnersCache with Spark with SingleStandardServer with Matchers {

   override protected def getNumEntries: Int = 10

   override def getCacheConfig: Option[String] = Some(
      """
        |{
        |    "local-cache":{
        |        "statistics":true,
        |        "encoding":{
        |            "key":{
        |                "media-type":"application/x-jboss-marshalling"
        |            },
        |            "value":{
        |                "media-type":"application/x-jboss-marshalling"
        |            }
        |        }
        |    }
        |}
        |""".stripMargin
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
