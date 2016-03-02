[![Build Status](https://travis-ci.org/erenavsarogullari/spark-hazelcast-connector.svg?branch=master)](https://github.com/erenavsarogullari/spark-hazelcast-connector)
[![Scala version](https://img.shields.io/badge/scala-2.11-orange.svg)](http://www.scala-lang.org/api/2.11.7/)
[![codecov.io](https://codecov.io/github/erenavsarogullari/spark-hazelcast-connector/coverage.svg?branch=master)](https://codecov.io/github/erenavsarogullari/spark-hazelcast-connector?branch=master)

## Apache Spark Hazelcast Connector

Spark-Hazelcast Connector API supports the following features :

### 1- Write Hazelcast Entries / Items to Spark as RDD.

##### * Distributed Map :
```
lazy val sc = new SparkContext(new SparkConf().setAppName("spark-hazelcast").setMaster("local[2]"))

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_distributed_map")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.IMap)

val hazelcastEntryRDD = new HazelcastEntryRDD[Int, String](sc, properties)
hazelcastEntryRDD.print()
```

##### * MultiMap :
```
lazy val sc = ...

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_multi_map")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.MultiMap)

val hazelcastEntryRDD = new HazelcastEntryRDD[Int, String](sc, properties)
hazelcastEntryRDD.print()
```

##### * ReplicatedMap :
```
lazy val sc = ...

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_replicated_map")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.ReplicatedMap)

val hazelcastEntryRDD = new HazelcastEntryRDD[Int, String](sc, properties)
hazelcastEntryRDD.print()
```

##### * Distributed List :
```
lazy val sc = ...

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_distributed_list")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.IList)

val hazelcastItemRDD = new HazelcastItemRDD[Int, String](sc, properties)
hazelcastItemRDD.print()
```

##### * Distributed Set :
```
lazy val sc = ...

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_distributed_set")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.ISet)

val hazelcastItemRDD = new HazelcastItemRDD[Int, String](sc, properties)
hazelcastItemRDD.print()
```

##### * Distributed Queue :
```
lazy val sc = ...

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_distributed_queue")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.IQueue)

val hazelcastItemRDD = new HazelcastItemRDD[Int, String](sc, properties)
hazelcastItemRDD.print()
```

### 2- Write Spark RDDs to Hazelcast Distributed Object :

##### * Distributed Map / MultiMap / ReplicatedMap :
```
lazy val sc = ...

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_distributed_map")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.IMap)

val dataList = (1 to 10).map(i => (i, s"value_$i")).toList
val tupleRDD = sc.parallelize[(Int, String)](dataList)

import com.otv.spark.hazelcast.connector.rdd.implicits._
tupleRDD.writeEntryToHazelcast(properties)
```

##### * Distributed List / Set / Queue :
```
val properties = ...

val intRDD = ...

import com.otv.spark.hazelcast.connector.rdd.implicits._
intRDD.writeItemToHazelcast(properties)
```


##### * Topic / Reliable Topic :
```
lazy val sc = ...

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_topic")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.ITopic)

val dataList = (1 to 10).toList
val intRDD = sc.parallelize[Int](dataList)

import com.otv.spark.hazelcast.connector.rdd.implicits._
intRDD.writeMessageToHazelcast(properties)
```

### 3- Write Hazelcast Entries / Items to Spark as DStream.

##### * Distributed Map / MultiMap / ReplicatedMap:
```
val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_distributed_map")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.IMap)

lazy val sc = new SparkContext(new SparkConf().setAppName("spark-hazelcast").setMaster("local[2]"))
lazy val ssc = new StreamingContext(sc, Seconds(1))

val hzMapStream = HazelcastUtils.createHazelcastEntryStream[Integer,String](ssc,
                                                                            StorageLevel.MEMORY_ONLY,
                                                                            sparkHazelcastProperties,
                                                                            Set(DistributedEventType.ADDED, DistributedEventType.REMOVED))
hzMapStream.print(10)

ssc.start()
```

##### * Distributed List / Set / Queue :
```
val properties = ...

lazy val ssc = ...

val hzListStream = HazelcastUtils.createHazelcastItemStream[String](ssc,
                                                                    StorageLevel.MEMORY_ONLY,
                                                                    sparkHazelcastProperties,
                                                                    Set(DistributedEventType.ADDED, DistributedEventType.REMOVED))
hzListStream.print(10)

ssc.start()
```

##### * Topic / Reliable Topic :
```
val properties = ...

lazy val ssc = ...

val hzListStream = HazelcastUtils.createHazelcastMessageStream[String](ssc,
                                                                        StorageLevel.MEMORY_ONLY,
                                                                        sparkHazelcastProperties)
hzListStream.print(10)

ssc.start()
```


### 4- Write Spark DStreams to Hazelcast Distributed Object :

##### * Distributed Map / MultiMap / ReplicatedMap :
```
lazy val sc = new SparkContext(new SparkConf().setAppName("spark-hazelcast").setMaster("local[2]"))
lazy val ssc = new StreamingContext(sc, Seconds(1))

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_distributed_map")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.IMap)

val rddQueue: Queue[RDD[(K, V)]] = ...

val queueStream = ssc.queueStream(rddQueue)

queueStream.writeEntryToHazelcast(properties)

ssc.start()

```

##### * Distributed List / Set / Queue :
```
lazy val ssc = ...

val properties = ...

val rddQueue: Queue[RDD[(K, V)]] = ...

val queueStream = ssc.queueStream(rddQueue)

queueStream.writeItemToHazelcast(properties)

ssc.start()
```

##### * Topic / Reliable Topic :
```
lazy val ssc = ...

val properties = ...

val rddQueue: Queue[RDD[(K, V)]] = ...

val queueStream = ssc.queueStream(rddQueue)

queueStream.writeMessageToHazelcast(properties)

ssc.start()
```
