package org.infinispan.spark.examples.twitter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import static org.infinispan.spark.examples.twitter.Sample.usage;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Properties;

/**
 * This demo will group tweets by country and print the top 20 countries, using Spark SQL support.
 *
 * @author gustavonalle
 */
public class SQLAggregationJava {

   public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
      if (args.length < 1) {
         usage(SQLAggregationJava.class.getSimpleName());
      }
      String infinispanHost = args[0];

      // Reduce the log level in the driver
      Logger.getLogger("org").setLevel(Level.WARN);

      SparkConf conf = Sample.getSparkConf("spark-infinispan-sql-aggregation-java");

      // Create java spark context
      JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

      // Populate infinispan properties
      Properties infinispanProperties = new Properties();
      infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost);

      // Create RDD from infinispan data
      JavaPairRDD<Long, Tweet> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(javaSparkContext, infinispanProperties);

      // Obtain the values only
      JavaRDD<Tweet> tweetsRDD = infinispanRDD.values();

      // Create a SQLContext, register a data frame and a temp table
      SQLContext sqlContext = new SQLContext(javaSparkContext);
      Dataset dataFrame = sqlContext.createDataFrame(tweetsRDD, Tweet.class);
      dataFrame.registerTempTable("tweets");

      // Run the Query and collect results
      Dataset<Row> rows = sqlContext.sql("SELECT country, count(*) as c from tweets WHERE country != 'N/A' GROUP BY country ORDER BY c desc");

      rows.takeAsList(20).forEach(System.out::println);
   }

}
