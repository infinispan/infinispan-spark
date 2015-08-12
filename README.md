### Infinispan Spark connector

[![Build Status](https://travis-ci.org/infinispan/infinispan-spark.svg)](https://travis-ci.org/infinispan/infinispan-spark)

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

* Apache Spark 1.4 or above
* Infinispan 8.0.0.Beta3 
* Java 8  
* Scala 2.10/2.11

#### Dependency:

Sbt:  

```"org.infinispan" %% "infinispan-spark" % "0.1-SNAPSHOT"```

Maven:

Scala 2.10  
```
<dependency>
    <groupId>org.infinispan</groupId>
    <artifactId>infinispan-spark_2.10</artifactId>
    <version>0.1-SNAPSHOT</scope>
</dependency>
```

Scala 2.11      
```
<dependency>
    <groupId>org.infinispan</groupId>
    <artifactId>infinispan-spark_2.11</artifactId>
    <version>0.1-SNAPSHOT</scope>
</dependency>
```

#### Build instructions

Package for Scala 2.11: ```./sbt package```  
Package for Scala 2.10 and 2.11: ```./sbt +package```  
Create examples uberjar: ```./sbt examples/assembly```  
Run all tests: ```./sbt +test```

#### Publishing

To publish to nexus, first export the credentials as environment variables:

```
export NEXUS_USER=...   
export NEXUS_PASS=...
```

#### Publishing a SNAPSHOT

``` ./sbt +publish ```

#### Releasing

``` ./sbt +release ```





