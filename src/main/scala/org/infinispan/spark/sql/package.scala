package org.infinispan.spark

import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession


package object sql {

   implicit class InfinispanSparkSession(session: SparkSession) {
      private val properties: Properties = new Properties

      def withInfinispanProperties(configuration: Properties): InfinispanSparkSession = {
         configuration.asScala.foreach { case (k, v) => properties.setProperty(k, v) }
         this
      }

      def getProperties: Properties = properties
   }

}
