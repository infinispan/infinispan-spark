### Infinispan Spark connector

[![Build Status](https://travis-ci.org/infinispan/infinispan-spark.svg?branch=master)](https://travis-ci.org/infinispan/infinispan-spark)

#### Supported:

* Write any key-value based RDD to Infinispan server
* Create an RDD from a Infinispan server cache
* Use Infinispan server side filters to create a cache based RDD
* Write any DStream to Infinispan server
* Create a DStream for events (insert, modify and delete) in a cache
* Spark serialiser based on JBoss Marshalling
* Scala 2.10.x and 2.11.x binaries
* Dataset API with push down predicates support
* Java API


#### Compatibility

| Version  | Infinispan | Spark | Scala | Java
| -------- | ---------- | ----- | ----- | ---- |
| 0.1  | 8.0.x  | 1.4.x | 2.10.x / 2.11.x | 8  |
| 0.2  | 8.1.x  | 1.5.x | 2.10.x / 2.11.x | 8  |
| 0.3  | 8.2.x 9.x | 1.6.x | 2.10.x / 2.11.x | 8  |
| 0.4  | 8.2.x 9.x | 2.0.0 | 2.10.x / 2.11.x | 8  |
| 0.5  | 8.2.x 9.x | 2.1.0 | 2.11.x | 8  |
| 0.6  | 8.2.x 9.x | 2.1.0 2.2.0 | 2.11.x | 8  |
| 0.7  | 9.2.1 | 2.3.x | 2.11.x | 8 |
| 0.8  | 9.3.x | 2.3.x | 2.11.x | 8 |
| 0.9 | 9.4.1 | 2.3.x | 2.11.x | 8 |


#### Dependency:

Sbt:  

```"org.infinispan" %% "infinispan-spark" % "0.9"```

Maven:

```
<dependency>
    <groupId>org.infinispan</groupId>
    <artifactId>infinispan-spark_2.11</artifactId>
    <version>0.9</version>
</dependency>
```

#### Configuration

The connector is configured using the ```org.infinispan.spark.config.ConnectorConfiguration``` class, the following methods are provided:

Method          | Description | Default
------------- | -------------|----------
setServerList(String) | List of servers | localhost:11222 | 
setCacheName(String) | The name of the Infinispan cache to be used in the computations | default cache | 
setAutoCreateCacheFromConfig(String) | Creates the cache with the supplied configuration in case it does not exist. The configuration is in XML format and follows the [Infinispan Embedded Schema](http://infinispan.org/schemas/infinispan-config-9.2.xsd) |
setAutoCreateCacheFromTemplate(String) | Creates the cache with a pre-existent [configuration template](http://infinispan.org/docs/stable/user_guide/user_guide.html#cache_configuration_templates) in case it does not exist. This setting is ignored if setAutoCreateCacheFromConfig() was previously called  |
setReadBatchSize(Integer)  | Batch size (number of entries) when reading from the cache | 10000 | 
setWriteBatchSize(Integer) | Batch size (number of entries) when writing to the cache | 500
setPartitions(Integer) | Number of partitions created per Infinispan server when processing data | 2
addProtoFile(String name, String contents) | Register a protobuf file describing one or more entities in the cache | Can be omitted if entities are [annotated](https://github.com/infinispan/infinispan/blob/master/client/hotrod-client/src/test/java/org/infinispan/client/hotrod/marshall/ProtoStreamMarshallerWithAnnotationsTest.java#L39) with protobuf encoding information. Protobuf encoding is required to filter the RDD by Query or to use the Dataset API
addMessageMarshaller(Class) | Registers a [Message Marshaller](http://infinispan.org/docs/dev/user_guide/user_guide.html#storing_protobuf_encoded_entities) for an entity in the cache | Can be omitted if entities are [annotated](https://github.com/infinispan/infinispan/blob/master/client/hotrod-client/src/test/java/org/infinispan/client/hotrod/marshall/ProtoStreamMarshallerWithAnnotationsTest.java#L39) with protobuf encoding information. Protobuf encoding is required to filter the RDD by Query or to use the Dataset API
addProtoAnnotatedClass(Class) | Registers a Class containing protobuf annotations such as @ProtoMessage and @ProtoField | Alternative to using ```addProtoFile``` and ```addMessageMarshaller``` methods, since both will be auto-generated based on the annotations.
setAutoRegisterProto() | Will cause automatically registration of protobuf schemas in the server. The schema can either be provided by ```addProtoFile()``` or inferred from the annotated classes registered with ```addProtoAnnotatedClass``` | no automatic registration is done
addHotRodClientProperty(key, value) | Used to configured extra Hot Rod client properties when contacting the Infinispan Server | |
setTargetEntity(Class) | Used in conjunction with the Dataset API to specify the Query target | If omitted, and in case there is only one class annotated with protobuf configured, it will choose that class
setKeyMarshaller(Class) | An implementation of ```org.infinispan.commons.marshall.Marshaller``` used to serialize and deserialize the keys | ```org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller```
setValueMarshaller(Class) | Same as ```keyMarshaller``` but for the values | same default as above
setKeyMediaType(String) | Specify an alternate [Media type](https://en.wikipedia.org/wiki/Media_type) to be used for keys during cache operations. Infinispan will convert the stored keys to this format when reading data. It is recommended to [configure the Media Type](http://infinispan.org/docs/stable/user_guide/user_guide.html#configuration) of the cache when planning to use this property | ```application/x-jboss-marshalling``` or ```application/x-protostream``` when using protofiles and message marshallers
setValueMediaType(String) | Same as ```keyMediaType``` but for values | same default as above

##### Connecting to secure servers


The following properties can be used via ```ConnectorConfiguration.addHotRodClientProperty(prop, value)``` in order to connect to Infinispan server with security enabled:


Property          | Description 
------------- | -------------
infinispan.client.hotrod.use_ssl | Enable SSL 
infinispan.client.hotrod.key_store_file_name | The JKS keystore file name, required when mutual SSL authentication is enabled in the Infinispan server. Can be either the file path or a class path resource. Examples: "/usr/local/keystore.jks", "classpath:/keystore.jks" | 
infinispan.client.hotrod.trust_store_file_name | The JKS keystore path or classpath containing server certificates | 
infinispan.client.hotrod.key_store_password | Password for the key store | 
infinispan.client.hotrod.trust_store_password | Password for the trust store |   

#### Basic usage:

#### Creating an RDD

##### Scala

```scala
import org.apache.spark.SparkContext
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.rdd.InfinispanRDD

val sc: SparkContext = new SparkContext()

val config = new ConnectorConfiguration().setCacheName("my-cache").setServerList("10.9.0.8:11222")

val infinispanRDD = new InfinispanRDD[String, MyEntity](sc, config)

val entitiesRDD = infinispanRDD.values
```

##### Java

```java
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

JavaSparkContext jsc = new JavaSparkContext();

ConnectorConfiguration config = new ConnectorConfiguration()
            .setCacheName("exampleCache").setServerList("server:11222");

JavaPairRDD<String, MyEntity> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, config);

JavaRDD<MyEntity> entitiesRDD = infinispanRDD.values();
```

#### Cache lifecycle control

##### Scala

```scala
import org.apache.spark.SparkContext
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.rdd.InfinispanRDD

val sc: SparkContext = new SparkContext()

// Automatically create distributed cache "tempCache" in the server
val config = new ConnectorConfiguration()
                    .setCacheName("myTempCache")
                    .setServerList("10.9.0.8:11222")
                    .setAutoCreateCacheFromConfig(<infinispan><cache-container><distributed-cache name="tempCache"/></cache-container></infinispan>.toString)

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

```

##### Java

```java
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

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
```

#### Creating an RDD Using a custom Splitter

##### Scala

```scala
import org.apache.spark.SparkContext
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.rdd._

val sc: SparkContext = ...

val config: ConnectorConfiguration = ...
val mySplitter = CustomSplitter()
val infinispanRDD = new InfinispanRDD[String, MyEntity](sc, config, mySplitter)
```

##### Java

```java
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

JavaSparkContext jsc = new JavaSparkContext();

ConnectorConfiguration config = new ConnectorConfiguration();

MySplitter customSplitter = new MySplitter();
InfinispanJavaRDD.createInfinispanRDD(jsc, config, customSplitter);
```

#### Creating a DStream

##### Scala

```scala
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.stream._

val sc = new SparkContext()
val config = new ConnectorConfiguration()
val ssc = new StreamingContext(sc, Seconds(1))
val stream = new InfinispanInputDStream[String, MyEntity](ssc, StorageLevel.MEMORY_ONLY, config)
```      

##### Java

```java
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.stream.InfinispanJavaDStream;
import static org.apache.spark.storage.StorageLevel.MEMORY_ONLY;

SparkConf conf = new SparkConf().setAppName("my-stream-app");

ConnectorConfiguration configuration = new ConnectorConfiguration();

JavaStreamingContext jsc = new JavaStreamingContext(conf, Seconds.apply(1));

InfinispanJavaDStream.createInfinispanInputDStream(jsc, MEMORY_ONLY(), configuration);
```

#### Filtering by a pre-built Query object

##### Scala

```scala
import org.infinispan.client.hotrod.{RemoteCache, Search}
import org.infinispan.spark.rdd.InfinispanRDD

val rdd: InfinispanRDD[String, MyEntity] = ???
val cache: RemoteCache[String, MyEntity] = ???

// Assuming MyEntity is already stored in the cache with protobuf encoding, and has protobuf annotations.
val query = Search.getQueryFactory(cache).from(classOf[MyEntity]).having("field").equal("value").build()

val filteredRDD = rdd.filterByQuery(query)
```

##### Java

```java
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

JavaSparkContext jsc = new JavaSparkContext();

ConnectorConfiguration conf = new ConnectorConfiguration();

InfinispanJavaRDD<String, MyEntity> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, conf);

RemoteCache<String, MyEntity> remoteCache = new RemoteCacheManager().getCache();

// Assuming MyEntity is already stored in the cache with protobuf encoding, and has protobuf annotations.
Query query = Search.getQueryFactory(remoteCache).from(MyEntity.class).having("field").equal("value").build();

JavaPairRDD<String, MyEntity> filtered = infinispanRDD.filterByQuery(query);
```

#### Filtering using Ickle Queries

##### Scala

```scala
import org.infinispan.spark.rdd.InfinispanRDD

val rdd: InfinispanRDD[String, MyEntity] = ???
val filteredRDD = rdd.filterByQuery("FROM MyEntity e where e.field BETWEEN 10 AND 20")
```

##### Java

```java
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

JavaSparkContext jsc = new JavaSparkContext();
ConnectorConfiguration conf = new ConnectorConfiguration();

InfinispanJavaRDD<String, MyEntity> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, conf);

JavaPairRDD<String, MyEntity> filtered = infinispanRDD.filterByQuery("From myEntity where field = 'value'");
```

#### Filtering by filter deployed in the Infinispan Server

##### Scala

```scala
import org.infinispan.spark.rdd.InfinispanRDD

val rdd: InfinispanRDD[String, MyEntity] = ???
// "my-filter-factory" filter and converts MyEntity to a Double, and has two parameters
val filteredRDD = rdd.filterByCustom[Double]("my-filter-factory", "param1", "param2")
```

##### Java

```java
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

JavaSparkContext jsc = new JavaSparkContext();

ConnectorConfiguration conf = new ConnectorConfiguration();

InfinispanJavaRDD<String, MyEntity> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, conf);

JavaPairRDD<String, MyEntity> filtered = infinispanRDD.filterByCustom("my-filter", "param1", "param2");
```

#### Write arbitrary key/value RDDs to Infinispan

##### Scala

```scala
import org.apache.spark.SparkContext
import org.infinispan.spark._
import org.infinispan.spark.config.ConnectorConfiguration

val config: ConnectorConfiguration = ???
val sc: SparkContext = ???

val simpleRdd = sc.parallelize(1 to 1000).zipWithIndex()
simpleRdd.writeToInfinispan(config)
```

##### Java

```java
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

JavaSparkContext jsc = new JavaSparkContext();

ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration();

List<Integer> numbers = IntStream.rangeClosed(1, 1000).boxed().collect(Collectors.toList());
JavaPairRDD<Integer, Long> numbersRDD = jsc.parallelize(numbers).zipWithIndex();

InfinispanJavaRDD.write(numbersRDD, connectorConfiguration);
```

#### Using SparkSQL

##### Scala

```scala
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
```

##### Java

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;

JavaSparkContext jsc = new JavaSparkContext();

ConnectorConfiguration conf = new ConnectorConfiguration();

// Obtain the values from an InfinispanRDD
JavaPairRDD<Long, MyEntity> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, conf);

JavaRDD<MyEntity> valuesRDD = infinispanRDD.values();

// Create a DataFrame from a SparkSession
SparkSession sparkSession = SparkSession.builder().config(new SparkConf().setMaster("masterHost")).getOrCreate();
Dataset<Row> dataFrame = sparkSession.createDataFrame(valuesRDD, MyEntity.class);

// Create a view
dataFrame.createOrReplaceTempView("myEntities");

// Create and run the Query
Dataset<Row> rows = sparkSession.sql("SELECT field1, count(*) as c from myEntities WHERE field1 != 'N/A' GROUP BY field1 ORDER BY c desc");
```

#### Using the DatasetAPI with support to push down predicates

##### Scala

```scala
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
```

##### Java

```java
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.infinispan.spark.config.ConnectorConfiguration;

import java.util.List;

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
```

#### Using multiple data formats

##### Java

```java
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.spark.config.ConnectorConfiguration;
import org.infinispan.spark.rdd.InfinispanJavaRDD;


JavaSparkContext jsc = new JavaSparkContext();

ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration()
           .setCacheName("exampleCache")
           .setValueMediaType(MediaType.APPLICATION_JSON_TYPE)
           .setValueMarshaller(UTF8StringMarshaller.class)
           .setServerList("server:11222");

JavaPairRDD<String, String> infinispanRDD = InfinispanJavaRDD.createInfinispanRDD(jsc, connectorConfiguration);

JavaRDD<String> jsonRDD = infinispanRDD.values();
```

##### Scala

```scala
import org.apache.spark.SparkContext
import org.infinispan.commons.dataconversion.MediaType
import org.infinispan.commons.marshall.UTF8StringMarshaller
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.rdd.InfinispanRDD

val sc: SparkContext = new SparkContext()

val config = new ConnectorConfiguration().setCacheName("my-cache").setServerList("10.9.0.8:11222")
    .setValueMediaType(MediaType.APPLICATION_JSON_TYPE)
    .setValueMarshaller(classOf[UTF8StringMarshaller])

// Values will be JSON represented as String
val jsonRDD = new InfinispanRDD[String, String](sc, config)
```

#### Build instructions

Package: ```./sbt package```  
Create examples uberjar: ```./sbt examples/assembly```  
Run all tests: ```./sbt test```
Create code coverage report: ```./sbt clean coverage test coverageReport```

#### Publishing

To publish to nexus, first export the credentials as environment variables:

```
export NEXUS_USER=...   
export NEXUS_PASS=...
```

#### Publishing a SNAPSHOT

``` ./sbt publish ```

#### Releasing

``` ./sbt release ```
