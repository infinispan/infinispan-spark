package org.infinispan.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.spark.domain.Address;
import org.infinispan.spark.domain.Person;
import org.infinispan.spark.rdd.InfinispanJavaRDD;
import scala.Tuple2;

import java.util.*;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author gustavonalle
 */
public class JavaApiTest {

   private static final Integer COUNT = 50;
   private static final List<String> COUNTRIES = Arrays.asList("BRA", "UK", "ITA", "FRA", "SPA");

   private final JavaSparkContext jsc;
   private final RemoteCache<Integer, Person> cache;
   private final Properties config;

    private InfinispanJavaRDD<Integer, Person> infinispanRDD;

   public JavaApiTest(JavaSparkContext jsc, RemoteCache<Integer, Person> cache, Properties config) {
      this.jsc = jsc;
      this.cache = cache;
      this.config = config;
      IntStream.rangeClosed(1, COUNT).forEach(i -> {
         String country = COUNTRIES.get(i % COUNTRIES.size());
         Address address = new Address("street", i, country);
         Person person = new Person("name" + i, (i / 10) * 10, address);
         cache.put(i, person);
      });
      infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, config);
   }

   public void testRDDRead() throws Exception {
      assertEquals(COUNT.intValue(), infinispanRDD.count());

      long olderThan30 = infinispanRDD.values().filter(p -> p.getAge() > 30).count();

      assertEquals(11, olderThan30);

      Map<String, Iterable<Person>> map = infinispanRDD
              .values()
              .groupBy(p -> p.getAddress().getCountry())
              .collectAsMap();

      assertEquals(new HashSet<>(COUNTRIES), map.keySet());
   }

   public void testRDDWrite() throws Exception {
      Person veryOld = new Person("james", 75, new Address("street", 12, "UK"));
      Person teenager = new Person("joan", 17, new Address("street", 12, "FRA"));
      List<Tuple2<Integer, Person>> pairs = Arrays.asList(
              new Tuple2<>(COUNT + 1, veryOld),
              new Tuple2<>(COUNT + 2, teenager)
      );

      JavaPairRDD<Integer, Person> pairsRDD = jsc.parallelizePairs(pairs);
      InfinispanJavaRDD.write(pairsRDD, config);

      assertEquals(COUNT + 2, cache.size());
   }

   public void testSQL() throws Exception {
      SQLContext sqlContext = new SQLContext(jsc);
      JavaRDD<Address> addressRDD = infinispanRDD.values().map(Person::getAddress);
      Dataset<Row> dataset = sqlContext.createDataFrame(addressRDD, Address.class);
      dataset.createOrReplaceTempView("addresses");

      List<Row> rows = sqlContext.sql("SELECT a.country from addresses a GROUP BY a.country").collectAsList();

      assertTrue(rows.stream().allMatch(r -> COUNTRIES.contains(r.getString(0))));
   }

    public void testFilterByDeployedFilter() throws Exception {
        int  minAge = 20;
        int maxAge = 30;
        JavaPairRDD<Integer, Person> filteredRDD = infinispanRDD.<Person>filterByCustom("age-filter", minAge, maxAge);

        long expected = infinispanRDD.values().filter(p -> p.getAge() >= minAge && p.getAge() <= maxAge).count();

        assertEquals(expected, filteredRDD.values().count());
    }

}
