package org.infinispan.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.domain.Runner;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author gustavonalle
 */
public class JavaProtobufTest {

   private final SparkSession sparkSession;
   private final RemoteCache<Integer, Runner> cache;
   private final ConnectorConfiguration config;
   private final InfinispanJavaRDD<Integer, Runner> infinispanRDD;

   public JavaProtobufTest(SparkSession session, RemoteCache<Integer, Runner> cache, ConnectorConfiguration config) {
      this.sparkSession = session;
      this.cache = cache;
      this.config = config;
      this.infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(new JavaSparkContext(sparkSession.sparkContext()), config);
   }

   public void testDataFrameApi() {
      Dataset<Row> rows = sparkSession.read().format("infinispan").options(config.toStringsMap()).load();

      assertEquals(cache.size(), rows.count());
   }

   public void testSQL() {
      String sql = "SELECT MIN(r.finishTimeSeconds) as time, first(r.name) as name, first(r.age) as age " +
            "FROM runner r " +
            "WHERE r.finished = true " +
            "GROUP BY r.age";

      Dataset<Row> infinispan = sparkSession.read().format("infinispan").options(config.toStringsMap()).load();

      infinispan.createOrReplaceTempView("runner");

      List<Row> result = sparkSession.sql(sql).collectAsList();

      result.forEach(row -> {
         Integer winnerTime = row.getAs("time");
         Integer age = row.getAs("age");
         Runner fastestOfAge = infinispanRDD.values()
               .filter(r -> r.getAge() == age && r.getFinished())
               .sortBy(Runner::getFinishTimeSeconds, true, config.getServerPartitions())
               .first();

         assertTrue(winnerTime == fastestOfAge.getFinishTimeSeconds());
      });
   }

   public void testFilterByQueryObject() throws Exception {
      Query remoteQuery = Search.getQueryFactory(cache).from(Runner.class).having("age").gt(30).build();
      JavaPairRDD<Integer, Runner> filteredRDD = infinispanRDD.filterByQuery(remoteQuery);

      assertTrue(filteredRDD.values().collect().stream().allMatch(p -> p.getAge() > 30));
   }

   public void testFilterByQueryString() throws Exception {
      JavaPairRDD<Integer, Runner> filteredRDD = infinispanRDD.filterByQuery("FROM runner r where r.age > 30");

      assertTrue(filteredRDD.values().collect().stream().allMatch(p -> p.getAge() > 30));
   }

}
