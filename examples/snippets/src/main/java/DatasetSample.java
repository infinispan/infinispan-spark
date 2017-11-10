import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.infinispan.spark.config.ConnectorConfiguration;

import java.util.List;

@SuppressWarnings("unused")
public class DatasetSample {

    public void sample() {
        // Configure the connector using the ConnectorConfiguration: register entities annotated with Protobuf,
        // and turn on automatic registration of schemas
        ConnectorConfiguration connectorConfig = new ConnectorConfiguration()
                .setServerList("server1:11222,server2:11222")
                .addProtoAnnotatedClass(User.class)
                .setAutoRegisterProto();

        // Create the SparkSession
        SparkSession sparkSession = SparkSession.builder().config(new SparkConf().setMaster("masterHost")).getOrCreate();

        // Load the "infinispan" datasource into a DataFame, using the infinispan config
        Dataset<Row> df = sparkSession.read().format("infinispan").options(connectorConfig.toStringsMap()).load();

        // From here it's possible to query using the DatasetSample API...
        List<Row> rows = df.filter(df.col("age").gt(30)).filter(df.col("age").lt(40)).collectAsList();

        // ... or execute SQL queries
        df.createOrReplaceTempView("user");
        String query = "SELECT first(r.name) as name, first(r.age) as age FROM user u GROUP BY r.age";
        List<Row> results = sparkSession.sql(query).collectAsList();
    }
}
