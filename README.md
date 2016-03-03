[![Build Status](https://travis-ci.org/erenavsarogullari/spark-hazelcast-connector.svg?branch=master)](https://github.com/erenavsarogullari/spark-hazelcast-connector)
[![Scala version](https://img.shields.io/badge/scala-2.11-orange.svg)](http://www.scala-lang.org/api/2.11.7/)
[![codecov.io](https://codecov.io/github/erenavsarogullari/spark-hazelcast-connector/coverage.svg?branch=master)](https://codecov.io/github/erenavsarogullari/spark-hazelcast-connector?branch=master)
[![License](http://img.shields.io/:license-Apache%202-red.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

## Apache Spark Hazelcast Connector

Spark-Hazelcast Connector API supports the following features :

1. Write Hazelcast Entries / Items to Spark as RDD.
2. Write Spark RDDs to Hazelcast as Distributed Object.
3. Write Hazelcast Entries / Items / Messages to Spark as DStream.
4. Write Spark DStreams to Hazelcast as Distributed Object.

Sample Hazelcast XML File as follows :
```
<hazelcast xsi:schemaLocation="http://www.hazelcast.com/schema/config hazelcast-config-3.6.xsd"
           xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <group>
        <name>dev</name>
        <password>dev-pass</password>
    </group>
    <instance-name>test_hazelcast_instance</instance-name>
    <network>
        <join>
            <multicast enabled="false"></multicast>
            <tcp-ip enabled="true">
                <member>127.0.0.1:5701</member>
            </tcp-ip>
        </join>
    </network>
</hazelcast>
```

### 1. Write Hazelcast Entries / Items to Spark as RDD :

##### 1.1 Distributed Map :
```
lazy val sc = new SparkContext(new SparkConf().setAppName("spark-hazelcast").setMaster("local[2]"))

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_distributed_map")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.IMap)

val hazelcastEntryRDD = new HazelcastEntryRDD[Int, String](sc, properties)
hazelcastEntryRDD.print()
```

##### 1.2 MultiMap :
```
lazy val sc = ...

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_multi_map")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.MultiMap)

val hazelcastEntryRDD = new HazelcastEntryRDD[Int, String](sc, properties)
hazelcastEntryRDD.print()
```

##### 1.3 ReplicatedMap :
```
lazy val sc = ...

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_replicated_map")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.ReplicatedMap)

val hazelcastEntryRDD = new HazelcastEntryRDD[Int, String](sc, properties)
hazelcastEntryRDD.print()
```

##### 1.4 Distributed List :
```
lazy val sc = ...

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_distributed_list")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.IList)

val hazelcastItemRDD = new HazelcastItemRDD[Int, String](sc, properties)
hazelcastItemRDD.print()
```

##### 1.5 Distributed Set :
```
lazy val sc = ...

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_distributed_set")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.ISet)

val hazelcastItemRDD = new HazelcastItemRDD[Int, String](sc, properties)
hazelcastItemRDD.print()
```

##### 1.6 Distributed Queue :
```
lazy val sc = ...

val properties = new Properties()
properties.put(HazelcastXMLConfigFileName, "hazelcast.xml")
properties.put(HazelcastDistributedObjectName, "test_distributed_queue")
properties.put(HazelcastDistributedObjectType, DistributedObjectType.IQueue)

val hazelcastItemRDD = new HazelcastItemRDD[Int, String](sc, properties)
hazelcastItemRDD.print()
```

### 2. Write Spark RDDs to Hazelcast as Distributed Object :

##### 2.1 Distributed Map / MultiMap / ReplicatedMap :
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

##### 2.2 Distributed List / Set / Queue :
```
val properties = ...

val intRDD = ...

import com.otv.spark.hazelcast.connector.rdd.implicits._
intRDD.writeItemToHazelcast(properties)
```

##### 2.3 Topic / Reliable Topic :
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

### 3. Write Hazelcast Entries / Items / Messages to Spark as DStream :

##### 3.1 Distributed Map / MultiMap / ReplicatedMap:
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

##### 3.2 Distributed List / Set / Queue :
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

##### 3.3 Topic / Reliable Topic :
```
val properties = ...

lazy val ssc = ...

val hzListStream = HazelcastUtils.createHazelcastMessageStream[String](ssc,
                                                                        StorageLevel.MEMORY_ONLY,
                                                                        sparkHazelcastProperties)
hzListStream.print(10)

ssc.start()
```


### 4. Write Spark DStreams to Hazelcast as Distributed Object :

##### 4.1 Distributed Map / MultiMap / ReplicatedMap :
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

##### 4.2 Distributed List / Set / Queue :
```
lazy val ssc = ...

val properties = ...

val rddQueue: Queue[RDD[(K, V)]] = ...

val queueStream = ssc.queueStream(rddQueue)

queueStream.writeItemToHazelcast(properties)

ssc.start()
```

##### 4.3 Topic / Reliable Topic :
```
lazy val ssc = ...

val properties = ...

val rddQueue: Queue[RDD[(K, V)]] = ...

val queueStream = ssc.queueStream(rddQueue)

queueStream.writeMessageToHazelcast(properties)

ssc.start()
```
