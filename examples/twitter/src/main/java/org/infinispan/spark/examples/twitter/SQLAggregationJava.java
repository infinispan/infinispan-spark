package org.infinispan.spark.examples.twitter;

import static org.infinispan.spark.examples.twitter.Sample.usage;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

/**
 * This demo will group tweets by country and print the top 20 countries, using Spark SQL support.
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

      // Create connector properties
      ConnectorConfiguration configuration = new ConnectorConfiguration().setServerList(infinispanHost);

      // Create java spark context
      JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

      // Create RDD from infinispan data
      JavaPairRDD<Long, Tweet> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(javaSparkContext, configuration);

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
