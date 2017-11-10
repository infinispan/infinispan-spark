import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

@SuppressWarnings("unused")
public class FilterByPreBuiltQuery {

    public void sample() {
        JavaSparkContext jsc = new JavaSparkContext();

        ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration()
                .setCacheName("exampleApp").setServerList("server:11222");

        InfinispanJavaRDD<String, MyEntity> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, connectorConfiguration);

        RemoteCache<String, MyEntity> remoteCache = new RemoteCacheManager().getCache();

        // Assuming MyEntity is already stored in the cache with protobuf encoding, and has protobuf annotations.
        Query query = Search.getQueryFactory(remoteCache).from(MyEntity.class).having("field").equal("some value").build();

        JavaPairRDD<String, MyEntity> filtered = infinispanRDD.filterByQuery(query);
    }
}
