import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.stream.InfinispanJavaDStream;

import static org.apache.spark.storage.StorageLevel.MEMORY_ONLY;

@SuppressWarnings("unused")
public class CreatingDStream {

   public void sample() {
      SparkConf conf = new SparkConf().setAppName("my-stream-app");

      ConnectorConfiguration configuration = new ConnectorConfiguration()
            .setServerList("myserver");

      JavaStreamingContext jsc = new JavaStreamingContext(conf, Seconds.apply(1));

      InfinispanJavaDStream.createInfinispanInputDStream(jsc, MEMORY_ONLY(), configuration);
   }
}
