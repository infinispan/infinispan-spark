import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

@SuppressWarnings("unused")
public class DifferentFormats {

    public void sample() {
        JavaSparkContext jsc = new JavaSparkContext();

        ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration()
                .setCacheName("exampleCache")
                .setValueMediaType(MediaType.APPLICATION_JSON_TYPE)
                .setValueMarshaller(UTF8StringMarshaller.class)
                .setServerList("server:11222");

        JavaPairRDD<String, String> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, connectorConfiguration);

        JavaRDD<String> jsonRDD = infinispanRDD.values();
    }
}
