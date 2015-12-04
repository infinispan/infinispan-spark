package org.infinispan.spark.examples.twitter

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.spark.rdd.InfinispanRDD

/**
 * This demo will group tweets by country and print the top 20 countries, using Spark SQL support.
 *
 * @author gustavonalle
 */
object SQLAggregationScala {

   def main(args: Array[String]) {
      if (args.length < 1) {
         System.out.println("Usage: SQLAggregationScala <infinispan_host>")
         System.exit(1)
      }

      Logger.getLogger("org").setLevel(Level.WARN)
      val infinispanHost = args(0)

      // Reduce the log level in the driver
      Logger.getLogger("org").setLevel(Level.WARN)

      // Create Spark Context
      val conf = new SparkConf().setAppName("spark-infinispan-rdd-aggregation-scala")
      val sc = new SparkContext(conf)

      // Populate infinispan properties
      val infinispanProperties = new Properties
      infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)

      // Create RDD from infinispan data
      val infinispanRDD = new InfinispanRDD[Long, Tweet](sc, configuration = infinispanProperties)

      // Create a SQLContext, register a data frame and a temp table
      val valuesRDD = infinispanRDD.values
      val sqlContext = new SQLContext(sc)
      val dataFrame = sqlContext.createDataFrame(valuesRDD, classOf[Tweet])
      dataFrame.registerTempTable("tweets")

      // Run the Query, collect and print results
      sqlContext.sql("SELECT country, count(*) as c from tweets WHERE country != 'N/A' GROUP BY country ORDER BY c desc")
            .collect().take(20).foreach(println)

   }

}

