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

#### Supported Configurations:

Name          | Description | Default
------------- | -------------|----------
infinispan.client.hotrod.server_list | List of servers | localhost:11222 | 
infinispan.rdd.cacheName  | The name of the cache that will back the RDD | default cache | 
infinispan.rdd.read_batch_size  | Batch size (number of entries) when reading from the cache | 10000 | 
infinispan.rdd.write_batch_size| Batch size (number of entries) when writing to the cache | 500
infinispan.rdd.number_server_partitions | Number of partitions created per Infinispan server | 2
infinispan.rdd.query.proto.protofiles | Map<String, String> with protobuf file names and contents | Can be ommited if entities are [annotated](https://github.com/infinispan/infinispan/blob/master/client/hotrod-client/src/test/java/org/infinispan/client/hotrod/marshall/ProtoStreamMarshallerWithAnnotationsTest.java#L39) with protobuf encoding information. Protobuf encoding is required to filter the RDD by Query
infinispan.rdd.query.proto.marshallers | List of protostream marshallers classes for the objects in the cache | Can be ommited if entities are [annotated](https://github.com/infinispan/infinispan/blob/master/client/hotrod-client/src/test/java/org/infinispan/client/hotrod/marshall/ProtoStreamMarshallerWithAnnotationsTest.java#L39) with protobuf encoding information. Protobuf encoding is required to filter the RDD by Query


#### Basic usage:

##### Creating an RDD

```scala
import java.util.Properties
import org.infinispan.spark.rdd._

val config = new Properties
config.put("infinispan.rdd.cacheName","my-cache")
config.put("infinispan.client.hotrod.server_list","10.9.0.8:11222")
val infinispanRDD = new InfinispanRDD[String, MyEntity](sc, configuration = config)
```

##### Creating a DStream

```scala
import org.infinispan.spark.stream._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import java.util.Properties

val sc = ... // Spark context
val props = ... // java.util.Properties with Infinispan RDD configuration
val ssc = new StreamingContext(sc, Seconds(1))
val stream = new InfinispanInputDStream[String, MyEntity](ssc, StorageLevel.MEMORY_ONLY, props)
```      

##### Filtering by Query

```scala
import org.infinispan.client.hotrod.{RemoteCacheManager, Search, RemoteCache}
import org.infinispan.spark.rdd.InfinispanRDD

val rdd: InfinispanRDD = ... 
val cache: RemoteCache = ...

// Assuming MyEntity is already stored in the cache with protobuf encoding, and has protobuf annotations.
val query = Search.getQueryFactory(cache).from(classOf[MyEntity]).having("field").equal("some value").toBuilder[RemoteQuery].build

val filteredRDD = rdd.filterByQuery(query, classOf[User])
```

##### Filtering by deployed filter in the Infinispan server

```scala
val rdd = InfinispanRDD[String,MyEntity] = .... 
// "my-filter-factory" filter and converts MyEntity to a Double, and has two parameters
val filteredRDD = rdd.filterByCustom[Double]("my-filter-factory", "param1", "param2")
```

##### Write arbitrary key/value RDDs to Infinispan

```scala
import java.util.Properties
import org.infinispan.spark._

val config = ...
val sc: SparkContext = ...

val simpleRdd = sc.parallelize((1 to 1000)).zipWithIndex()
simpleRdd.writeToInfinispan(config) 
```

#### Build instructions

Package for Scala 2.11: ```./sbt package```  
Package for Scala 2.10 and 2.11: ```./sbt +package```  
Create examples uberjar: ```./sbt +examples/assembly```  
Run all tests: ```./sbt +test```
Create code coverage report: ```./sbt clean coverage test coverageReport```

#### Publishing

To publish to nexus, first export the credentials as environment variables:

```
export NEXUS_USER=...   
export NEXUS_PASS=...
```

#### Publishing a SNAPSHOT

``` ./sbt +publish ```

#### Releasing

``` ./sbt release ```





