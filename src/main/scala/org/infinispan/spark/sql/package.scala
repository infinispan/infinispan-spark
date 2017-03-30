package org.infinispan.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession


package object sql {

   implicit class InfinispanSparkSession(session: SparkSession) {
      private val properties: Properties = new Properties

      def withInfinispanProperties(configuration: Properties) = {
         properties.putAll(configuration)
         this
      }

      def getProperties = properties
   }

}
