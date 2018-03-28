import org.infinispan.spark.rdd.Splitter

class MyEntity

class MySplitter extends Splitter {

  import org.infinispan.client.hotrod.CacheTopologyInfo
  import org.infinispan.spark.config.ConnectorConfiguration

  override def split(cacheTopology: CacheTopologyInfo, properties: ConnectorConfiguration) = ???
}

class Samples {
  def creatingAnRDD(): Unit = {
    import org.apache.spark.SparkContext
    import org.infinispan.spark.config.ConnectorConfiguration
    import org.infinispan.spark.rdd.InfinispanRDD

    val sc: SparkContext = new SparkContext()

    val config = new ConnectorConfiguration().setCacheName("my-cache").setServerList("10.9.0.8:11222")

    val infinispanRDD = new InfinispanRDD[String, MyEntity](sc, config)

    val entitiesRDD = infinispanRDD.values
  }

  def cacheAdmin(): Unit = {
    import org.apache.spark.SparkContext
    import org.infinispan.spark.config.ConnectorConfiguration
    import org.infinispan.spark.rdd.InfinispanRDD

    val sc: SparkContext = new SparkContext()

    // Automatically create distributed cache "tempCache" in the server
    val config = new ConnectorConfiguration()
       .setCacheName("myTempCache")
       .setServerList("10.9.0.8:11222")
       .setAutoCreateCacheFromConfig(<infinispan><cache-container><distributed-cache name="tempCache"/></cache-container></infinispan>.toString())

    val infinispanRDD = new InfinispanRDD[String, MyEntity](sc, config)

    val entitiesRDD = infinispanRDD.values

    // Obtain the cache admin object
    val cacheAdmin = infinispanRDD.cacheAdmin()

    // Check if cache exists
    cacheAdmin.exists("tempCache")

    // Clear cache
    cacheAdmin.clear("tempCache")

    // Delete cache
    cacheAdmin.delete("tempCache")
  }

  def splitter(): Unit = {
    import org.apache.spark.SparkContext
    import org.infinispan.spark.config.ConnectorConfiguration
    import org.infinispan.spark.rdd._

    val sc: SparkContext = ???

    val config: ConnectorConfiguration = ???
    val mySplitter = new MySplitter()
    val infinispanRDD = new InfinispanRDD[String, MyEntity](sc, config, mySplitter)

  }

  def dstream(): Unit = {
    import org.apache.spark.SparkContext
    import org.apache.spark.storage.StorageLevel
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    import org.infinispan.spark.config.ConnectorConfiguration
    import org.infinispan.spark.stream._

    val sc = new SparkContext()
    val config = new ConnectorConfiguration()
    val ssc = new StreamingContext(sc, Seconds(1))
    val stream = new InfinispanInputDStream[String, MyEntity](ssc, StorageLevel.MEMORY_ONLY, config)
  }

  def filterByPreBuiltQueryObject(): Unit = {
    import org.infinispan.client.hotrod.{RemoteCache, Search}
    import org.infinispan.spark.rdd.InfinispanRDD

    val rdd: InfinispanRDD[String, MyEntity] = ???
    val cache: RemoteCache[String, MyEntity] = ???

    // Assuming MyEntity is already stored in the cache with protobuf encoding, and has protobuf annotations.
    val query = Search.getQueryFactory(cache).from(classOf[MyEntity]).having("field").equal("some value").build()

    val filteredRDD = rdd.filterByQuery(query)

  }

  def filterByIckleQuery(): Unit = {
    import org.infinispan.spark.rdd.InfinispanRDD

    val rdd: InfinispanRDD[String, MyEntity] = ???

    val filteredRDD = rdd.filterByQuery("FROM MyEntity e where e.field BETWEEN 10 AND 20")
  }

  def filterByDeployed(): Unit = {
    import org.infinispan.spark.rdd.InfinispanRDD

    val rdd: InfinispanRDD[String, MyEntity] = ???
    // "my-filter-factory" filter and converts MyEntity to a Double, and has two parameters
    val filteredRDD = rdd.filterByCustom[Double]("my-filter-factory", "param1", "param2")
  }

  def writeArbitraryKV(): Unit = {
    import org.apache.spark.SparkContext
    import org.infinispan.spark._
    import org.infinispan.spark.config.ConnectorConfiguration

    val config: ConnectorConfiguration = ???
    val sc: SparkContext = ???

    val simpleRdd = sc.parallelize(1 to 1000).zipWithIndex()
    simpleRdd.writeToInfinispan(config)
  }

  def useSparkSQL(): Unit = {
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.{SparkConf, SparkContext}
    import org.infinispan.spark.config.ConnectorConfiguration
    import org.infinispan.spark.rdd._

    val sc: SparkContext = ???

    val config = new ConnectorConfiguration().setServerList("myserver1:port,myserver2:port")

    // Obtain the values from an InfinispanRDD
    val infinispanRDD = new InfinispanRDD[Long, MyEntity](sc, config)
    val valuesRDD = infinispanRDD.values

    // Create a DataFrame from a SparkSession
    val sparkSession = SparkSession.builder().config(new SparkConf().setMaster("masterHost")).getOrCreate()
    val dataFrame = sparkSession.createDataFrame(valuesRDD, classOf[MyEntity])

    // Create a view
    dataFrame.createOrReplaceTempView("myEntities")

    // Create and run the Query, collect and print results
    sparkSession.sql("SELECT field1, count(*) as c from myEntities WHERE field1 != 'N/A' GROUP BY field1 ORDER BY c desc")
      .collect().take(20).foreach(println)
  }

  def dataSetPushdown(): Unit = {
    import org.apache.spark._
    import org.apache.spark.sql._
    import org.infinispan.protostream.annotations.{ProtoField, ProtoMessage}
    import org.infinispan.spark.config.ConnectorConfiguration

    import scala.annotation.meta.beanGetter
    import scala.beans.BeanProperty

    /**
      * Entities can be annotated in order to automatically generate protobuf schemas.
      * Also, they should be valid java beans. From Scala this can be achieved as:
      */
    @ProtoMessage(name = "user")
    class User(@(ProtoField@beanGetter)(number = 1, required = true) @BeanProperty var name: String,
               @(ProtoField@beanGetter)(number = 2, required = true) @BeanProperty var age: Int) {

      def this() = {
        this(name = "", age = -1)
      }
    }

    // Configure the connector using the ConnectorConfiguration: register entities annotated with Protobuf,
    // and turn on automatic registration of schemas
    val infinispanConfig: ConnectorConfiguration = new ConnectorConfiguration()
      .setServerList("server1:11222,server2:11222")
      .addProtoAnnotatedClass(classOf[User])
      .setAutoRegisterProto()

    // Create the SparkSession
    val sparkSession = SparkSession.builder().config(new SparkConf().setMaster("masterHost")).getOrCreate()

    // Load the "infinispan" datasource into a DataFame, using the infinispan config
    val df = sparkSession.read.format("infinispan").options(infinispanConfig.toStringsMap).load()

    // From here it's possible to query using the DatasetSample API...
    val rows: Array[Row] = df.filter(df("age").gt(30)).filter(df("age").lt(40)).collect()

    // ... or execute SQL queries
    df.createOrReplaceTempView("user")
    val query = "SELECT first(r.name) as name, first(r.age) as age FROM user u GROUP BY r.age"
    val rowsFromSQL: Array[Row] = sparkSession.sql(query).collect()
  }
}
