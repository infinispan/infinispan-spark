package org.infinispan.spark.suites

import org.infinispan.client.hotrod.impl.ConfigurationProperties._
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.domain.{AddressMarshaller, PersonMarshaller, Runner}
import org.scalatest.{FunSuite, Matchers}

class PropertiesSuite extends FunSuite with Matchers {

   test("to StringMap and back") {

      val cfg = <infinispan>
         <cache-container>
            <local-cache name="test"/>
         </cache-container>
      </infinispan>.toString()

      val initialConfig = new ConnectorConfiguration()
        .setCacheName("test")
        .setServerList("localhost:1234,localhost:2345")
        .setTargetEntity(classOf[Runner])
        .setPartitions(10)
        .setAutoRegisterProto()
        .setKeyStoreFileName("fileName")
        .setKeyStorePassword("changeme")
        .addMessageMarshaller(classOf[PersonMarshaller])
        .addMessageMarshaller(classOf[AddressMarshaller])
        .addProtoFile("file1", "message A {}")
        .addProtoFile("file2", "message B {}")
        .addHotRodClientProperty("infinispan.client.test", "value")
        .setAutoCreateCacheFromConfig(cfg)

      val stringMap = initialConfig.toStringsMap

      stringMap(ConnectorConfiguration.CacheName) shouldBe "test"
      stringMap(SERVER_LIST) shouldBe "localhost:1234,localhost:2345"
      stringMap(ConnectorConfiguration.TargetEntity) shouldBe "org.infinispan.spark.domain.Runner"
      stringMap(ConnectorConfiguration.AutoCreateCacheConfig) shouldBe cfg

      val converted = ConnectorConfiguration(stringMap)

      converted.getCacheName shouldBe initialConfig.getCacheName
      converted.getServerList shouldBe initialConfig.getServerList
      converted.getTargetEntity shouldBe initialConfig.getTargetEntity
      converted.getAutoCreateCacheFromConfig shouldBe cfg
   }

}
