import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

@SuppressWarnings("unused")
public class FilterByIckle {

    public void sample() {
        JavaSparkContext jsc = new JavaSparkContext();

        ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration()
                .setCacheName("exampleApp").setServerList("server:11222");

        InfinispanJavaRDD<String, MyEntity> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, connectorConfiguration);

        JavaPairRDD<String, MyEntity> filtered = infinispanRDD.filterByQuery("From myEntity where field = 'some value'");
    }
}
