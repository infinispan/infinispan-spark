import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.spark.CacheAdmin;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

@SuppressWarnings("unused")
public class CacheAdminSample {

    public void sample() {
        JavaSparkContext jsc = new JavaSparkContext();

        ConnectorConfiguration config = new ConnectorConfiguration()
                .setCacheName("exampleCache").setServerList("server:11222")
                .setAutoCreateCacheFromConfig("<infinispan><cache-container><distributed-cache name=\"tempCache\"/></cache-container></infinispan>");

        InfinispanJavaRDD<String, MyEntity> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, config);

        JavaRDD<MyEntity> entitiesRDD = infinispanRDD.values();

        // Obtain the cache admin object
        CacheAdmin cacheAdmin = infinispanRDD.cacheAdmin();

        // Check if cache exists
        cacheAdmin.exists("tempCache");

        // Clear cache
        cacheAdmin.clear("tempCache");

        // Delete cache
        cacheAdmin.delete("tempCache");
    }
}