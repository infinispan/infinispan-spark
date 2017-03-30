package org.infinispan.spark.suites

import org.infinispan.spark.domain.User
import org.infinispan.spark.test._
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

@DoNotDiscover
class DatasetWithScalaEntitySuite extends FunSuite with UsersCache with Spark with MultipleServers with Matchers
  with DatasetAssertions[User] {

   override protected def getNumEntries: Int = 100

   override def getCacheType = CacheType.DISTRIBUTED

   override def getConfiguration = {
      super.getConfiguration
        .addProtoAnnotatedClass(classOf[User])
        .setAutoRegisterProto()
        .setTargetEntity(classOf[User])
   }

   test("read data using the DataFrame API") {
      val df = getSparkSession.read.format("infinispan").options(getConfiguration.toStringsMap).load()

      val filter = df.filter(df("age").gt(30)).filter(df("age").lt(40))

      assertDataset(filter, r => r.getAge > 30 && r.getAge < 40)
   }

   override def row2String(e: User): String = e.name
}
