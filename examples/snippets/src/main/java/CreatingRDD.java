import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

@SuppressWarnings("unused")
public class CreatingRDD {

   public void sample() {
      JavaSparkContext jsc = new JavaSparkContext();

      ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration()
            .setCacheName("exampleCache").setServerList("server:11222");

      JavaPairRDD<String, MyEntity> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, connectorConfiguration);

      JavaRDD<MyEntity> entitiesRDD = infinispanRDD.values();

   }

}
