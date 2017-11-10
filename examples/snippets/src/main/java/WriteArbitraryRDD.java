import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("unused")
public class WriteArbitraryRDD {

    public void sample() {
        JavaSparkContext jsc = new JavaSparkContext();

        ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration();

        List<Integer> numbers = IntStream.rangeClosed(1, 1000).boxed().collect(Collectors.toList());
        JavaPairRDD<Integer, Long> numbersRDD = jsc.parallelize(numbers).zipWithIndex();

        InfinispanJavaRDD.write(numbersRDD, connectorConfiguration);
    }
}
