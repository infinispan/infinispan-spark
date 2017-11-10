import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

@SuppressWarnings("unused")
public class CustomSplitter {

   public void sample() {
      JavaSparkContext jsc = new JavaSparkContext();

      ConnectorConfiguration config = new ConnectorConfiguration();

      MySplitter customSplitter = new MySplitter();
      InfinispanJavaRDD.createInfinispanRDD(jsc, config, customSplitter);

   }
}
