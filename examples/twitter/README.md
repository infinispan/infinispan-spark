## Twitter Demo

This sample shows the integration between Infinispan and Spark including RDD operations, writing from RDDs, Spark SQL 
and Streaming.

### 0. Preparation

#### Build the project

Make sure project is built to generate the job fat jar. From the project root, type:

```./sbt +examples/assembly```

#### MacOS X docker users

Create the following route in order to make the containers acessible via their IPs:

``` sudo route -n add 172.17.0.0/16 `docker-machine ip default` ```

#### Launch a cluster

Make sure ```docker``` and ```docker-compose``` are installed.  
To launch the Spark standalone cluster and the Infinispan cluster, run the script: 

``` ./run-clusters.sh ```

#### Obtain Tweeter OAuth credentials

Since Spark twitter connector requires authentication, credentials need to be obtained from the twitter account holder by 
following instructions on https://dev.twitter.com/oauth/overview/application-owner-access-tokens

### 1. Populate the cache

The first job ```TwitterStreamConsumer``` will create a Twitter DStream, apply a transformation and save them to Infinispan.

To run the job:

```./run-job.sh org.infinispan.spark.examples.twitter.StreamConsumerJava <twitter.consumerKey> <twitter.consumerSecret> <twitter.accessToken> <twitter.accessTokenSecret>```

or use ```org.infinispan.spark.examples.twitter.TwitterStreamConsumerScala``` to run the Scala version

After a few seconds, the number of tweets inserted in the cache plus the last tweet stored will be printed.

```
3262 tweets inserted in the cache
Last tweet:RT @Teeeeeago: Gostei de um vídeo @YouTube de @chefejinbe http://t.co/dfEpYK1QUm #Bejiin - ResourcePacks #4
```

To interrupt the job, hit CTRL+C

### 2. Counting relevant words

The ```WordCount``` job will run a map reduce job on all data currently in the cache, counting words in the Tweets discarding common used ones
and will print the top 20 words found. Output will be:

```
'love' appears 96 times                                                         
'July' appears 66 times
'time' appears 62 times
```

To run the job:

```./run-job.sh org.infinispan.spark.examples.twitter.WordCountJava <twitter.consumerKey> <twitter.consumerSecret> <twitter.accessToken> <twitter.accessTokenSecret>```

or use ```org.infinispan.spark.examples.twitter.WordCountScala``` for the Scala version.

### 3. Aggregation using SQL

The ```SQLAggregation``` job uses SQL to group the tweets by country and print the top 20 countries:

```SELECT country, count(*) as c from tweets WHERE country != 'N/A' GROUP BY country ORDER BY c desc```

Java:

```./run-job.sh org.infinispan.spark.examples.twitter.SQLAggregationJava <twitter.consumerKey> <twitter.consumerSecret> <twitter.accessToken> <twitter.accessTokenSecret>```

or use ```org.infinispan.spark.examples.twitter.SQLAggregationScala``` for the Scala version.


Sample output:

```
[United States,21]
[Indonesia,21]
[Türkiye,18]
[日本,17]
[Brasil,17]
[United Kingdom,17]
```

### 4. Listen to events from the cache

The ```StreamProducer``` job extends the ```StreamConsumer``` and also create a InfinispanInputDStream based on the data inserted
in the cache. It prints the summary by country of the tweets inserted in the cache, by using a sliding window of 60 seconds

Java:

```./run-job.sh org.infinispan.spark.examples.twitter.StreamProducerJava <twitter.consumerKey> <twitter.consumerSecret> <twitter.accessToken> <twitter.accessTokenSecret>```

or use ```org.infinispan.spark.examples.twitter.StreamProducerScala``` for the Scala version.

Sample output:

```
---------- 1437480561000 ms ----------                                          
[United Kingdom,5]
[ประเทศไทย,5]
[日本,5]
[Argentina,4]
[Republika ng Pilipinas,4]
[Indonesia,3]
[Brasil,3]
[United States,3]
```

### 5. Stop containers

```docker-compose stop```
