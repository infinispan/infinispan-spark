package org.infinispan.spark.examples.twitter

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.infinispan.spark.examples.twitter.Sample.{getSparkConf, usage}
import org.infinispan.spark.rdd.InfinispanRDD

/**
 * This demo will group tweets by country and print the top 20 countries, using Spark SQL support.
 *
 * @author gustavonalle
 */
object SQLAggregationScala {

   def main(args: Array[String]) {
      if (args.length < 1) {
         usage("SQLAggregationScala")
      }

      Logger.getLogger("org").setLevel(Level.WARN)
      val infinispanHost = args(0)

      // Reduce the log level in the driver
      Logger.getLogger("org").setLevel(Level.WARN)

      // Create Spark Context
      val conf = getSparkConf("spark-infinispan-rdd-aggregation-scala")
      val sc = new SparkContext(conf)

      // Populate infinispan properties
      val infinispanProperties = new Properties
      infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)


      // Create RDD from infinispan data
      val infinispanRDD = new InfinispanRDD[Long, Tweet](sc, configuration = infinispanProperties)

      // Create a SQLContext, register a data frame and a temp table
      val valuesRDD = infinispanRDD.values
      val sparkSession = SparkSession.builder().config(conf).getOrCreate()
      val dataFrame = sparkSession.createDataFrame(valuesRDD, classOf[Tweet])
      dataFrame.createOrReplaceTempView("tweets")

      // Run the Query, collect and print results
      sparkSession.sql("SELECT country, count(*) as c from tweets WHERE country != 'N/A' GROUP BY country ORDER BY c desc")
              .collect().take(20).foreach(println)

   }

}

