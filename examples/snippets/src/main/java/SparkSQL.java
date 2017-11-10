import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

@SuppressWarnings("unused")
public class SparkSQL {

    public void sample() {
        JavaSparkContext jsc = new JavaSparkContext();

        ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration();

        // Obtain the values from an InfinispanRDD
        JavaPairRDD<Long, MyEntity> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, connectorConfiguration);

        JavaRDD<MyEntity> valuesRDD = infinispanRDD.values();

        // Create a DataFrame from a SparkSession
        SparkSession sparkSession = SparkSession.builder().config(new SparkConf().setMaster("masterHost")).getOrCreate();
        Dataset<Row> dataFrame = sparkSession.createDataFrame(valuesRDD, MyEntity.class);

        // Create a view
        dataFrame.createOrReplaceTempView("myEntities");

        // Create and run the Query
        Dataset<Row> rows = sparkSession.sql("SELECT field1, count(*) as c from myEntities WHERE field1 != 'N/A' GROUP BY field1 ORDER BY c desc");
    }
}
