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


#### Dependency:

Sbt:  

```"org.infinispan" %% "infinispan-spark" % "0.4"```

Maven:

Scala 2.11  
```
<dependency>
    <groupId>org.infinispan</groupId>
    <artifactId>infinispan-spark_2.11</artifactId>
    <version>0.4</version>
</dependency>
```

Scala 2.10  
```
<dependency>
    <groupId>org.infinispan</groupId>
    <artifactId>infinispan-spark_2.10</artifactId>
    <version>0.4</version>
</dependency>
```

#### Configuration

The connector is configured using the ```org.infinispan.spark.config.ConnectorConfiguration``` class, the following methods are provided:

Method          | Description | Default
------------- | -------------|----------
setServerList(String) | List of servers | localhost:11222 | 
setCacheName(String) | The name of the Infinispan cache to be used in the computations | default cache | 
setReadBatchSize(Integer)  | Batch size (number of entries) when reading from the cache | 10000 | 
setWriteBatchSize(Integer) | Batch size (number of entries) when writing to the cache | 500
setPartitions(Integer) | Number of partitions created per Infinispan server when processing data | 2
addProtoFile(String name, String contents) | Register a protobuf file describing one or more entities in the cache | Can be omitted if entities are [annotated](https://github.com/infinispan/infinispan/blob/master/client/hotrod-client/src/test/java/org/infinispan/client/hotrod/marshall/ProtoStreamMarshallerWithAnnotationsTest.java#L39) with protobuf encoding information. Protobuf encoding is required to filter the RDD by Query or to use the Dataset API
addMessageMarshaller(Class) | Registers a [Message Marshaller](http://infinispan.org/docs/dev/user_guide/user_guide.html#storing_protobuf_encoded_entities) for an entity in the cache | Can be omitted if entities are [annotated](https://github.com/infinispan/infinispan/blob/master/client/hotrod-client/src/test/java/org/infinispan/client/hotrod/marshall/ProtoStreamMarshallerWithAnnotationsTest.java#L39) with protobuf encoding information. Protobuf encoding is required to filter the RDD by Query or to use the Dataset API
addProtoAnnotatedClass(Class) | Registers a Class containing protobuf annotations such as @ProtoMessage and @ProtoField | Alternative to using ```addProtoFile``` and ```addMessageMarshaller``` methods, since both will be auto-generated based on the annotations.
setAutoRegisterProto() | Will cause automatically registration of protobuf schemas in the server. The schema can either be provided by ```addProtoFile()``` or inferred from the annotated classes registered with ```addProtoAnnotatedClass``` | no automatic registration is done
addHotRodClientProperty(key, value) | Used to configured extra Hot Rod client properties when contacting the Infinispan Server | |
setTargetEntity(Class) | Used in conjunction with the Dataset API to specify the Query target | If omitted, and in case there is only one class annotated with protobuf configured, it will choose that class

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

##### Creating an RDD

```scala
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.rdd._

val sc: SparkContext =  ...

val config = new ConnectorConfiguration().setCacheName("my-cache").setServerList("10.9.0.8:11222")

val infinispanRDD = new InfinispanRDD[String, MyEntity](sc, config)
```

##### Creating an RDD Using a custom Splitter

```scala
import org.infinispan.spark.rdd._

val config =  ...
val mySplitter = new CustomSplitter(...)
val infinispanRDD = new InfinispanRDD[String, MyEntity](sc, config, mySplitter)
```


##### Creating a DStream

```scala
import org.infinispan.spark.stream._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.infinispan.spark.config.ConnectorConfiguration

val sc = ... // Spark context
val config = ... // ConnectorConfiguration with Infinispan RDD configuration
val ssc = new StreamingContext(sc, Seconds(1))
val stream = new InfinispanInputDStream[String, MyEntity](ssc, StorageLevel.MEMORY_ONLY, config)
```      

##### Filtering by a pre-built Query object

```scala
import org.infinispan.client.hotrod.{RemoteCacheManager, Search, RemoteCache}
import org.infinispan.spark.rdd.InfinispanRDD

val rdd: InfinispanRDD = ... 
val cache: RemoteCache = ...

// Assuming MyEntity is already stored in the cache with protobuf encoding, and has protobuf annotations.
val query = Search.getQueryFactory(cache).from(classOf[MyEntity]).having("field").equal("some value").build()

val filteredRDD = rdd.filterByQuery(query)
```

##### Filtering using Ickle Queries

```scala
import org.infinispan.client.hotrod.{RemoteCacheManager, Search, RemoteCache}
import org.infinispan.spark.rdd.InfinispanRDD

val rdd: InfinispanRDD = ...

val filteredRDD = rdd.filterByQuery("FROM MyEntity e where e.field BETWEEN 10 AND 20")
```

##### Filtering by deployed filter in the Infinispan server

```scala
val rdd = InfinispanRDD[String,MyEntity] = .... 
// "my-filter-factory" filter and converts MyEntity to a Double, and has two parameters
val filteredRDD = rdd.filterByCustom[Double]("my-filter-factory", "param1", "param2")
```

##### Write arbitrary key/value RDDs to Infinispan

```scala
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark._

val config: ConnectorConfiguration = ...
val sc: SparkContext = ...

val simpleRdd = sc.parallelize((1 to 1000)).zipWithIndex()
simpleRdd.writeToInfinispan(config) 
```


##### Using the DatasetAPI with support to push down predicates

```scala

import org.infinispan.protostream.annotations.{ProtoField, ProtoMessage}
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

```

```scala
import org.infinispan.spark.config.ConnectorConfiguration
import org.apache.spark._
import org.apache.spark.sql._

// Configure the connector using the ConnectorConfiguration: register entities annotated with Protobuf, 
// and turn on automatic registration of schemas
val infinispanConfig: ConnectorConfiguration = new ConnectorConfiguration()
         .setServerList("server1:11222,server2:11222")
         .addProtoAnnotatedClass(classOf[User])
         .setAutoRegisterProto()

// Create the SparkSession
val sparkSession = SparkSession.builder().config(new SparkConf().setMaster("masterHost")).getOrCreate()

// Load the "infinispan" datasource into a DataFame, using the infinispan config 
val df: DataFrame = sparkSession.read.format("infinispan").options(infinispanConfig.toStringsMap).load()

// From here it's possible to query using the Dataset API...
val rows: Array[Row] = df.filter(df("age").gt(30)).filter(df("age").lt(40)).collect()

// ... or execute SQL queries
df.createOrReplaceTempView("user")

val query = "SELECT first(r.name) as name, first(r.age) as age FROM user u GROUP BY r.age"

val rowsFromSQL: Array[Row] = sparkSession.sql(query).collect()

```

#### Build instructions

Package for Scala 2.11: ```./sbt package```  
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





