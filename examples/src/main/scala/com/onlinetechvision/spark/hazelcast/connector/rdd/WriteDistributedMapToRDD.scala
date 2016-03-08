package com.onlinetechvision.spark.hazelcast.connector.rdd

import java.util.Properties

import com.hazelcast.config.ClasspathXmlConfig
import com.hazelcast.core._
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastConfig._
import com.onlinetechvision.spark.hazelcast.connector.{DistributedObjectType, User}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by eren.avsarogullari on 3/8/16.
  */
object WriteDistributedMapToRDD {

  def main(args: Array[String]) {

    val HazelcastXMLFileName = "hazelcast_config.xml"
    val HazelcastDistributedMapName = "test_distributed_map"

    // Spark Context is created...
    val sc = new SparkContext(new SparkConf().setAppName("spark-hazelcast").setMaster("local"))

    // Distributed Map is created with the content...
    val hzInstance = Hazelcast.getOrCreateHazelcastInstance(new ClasspathXmlConfig(HazelcastXMLFileName))
    val distributedMap: IMap[Int,User] = hzInstance.getMap(HazelcastDistributedMapName)
    (1 to 100).foreach(index => distributedMap.put(index, new User(index, s"name$index", s"surname$index")))

    // Spark Hazelcast properties are created...
    val sparkHazelcastProperties = new Properties()
    sparkHazelcastProperties.put(HazelcastXMLConfigFileName, HazelcastXMLFileName)
    sparkHazelcastProperties.put(HazelcastDistributedObjectName, HazelcastDistributedMapName)
    sparkHazelcastProperties.put(HazelcastDistributedObjectType, DistributedObjectType.IMap)

    // Distributed Map is written to Spark as a RDD...
    val userRDD = new HazelcastEntryRDD[Int,User](sc, sparkHazelcastProperties)
    println(s"userRDD has got ${userRDD.count} elements.")

    // Prints elements of 'userRDD'
    userRDD.foreach(println)

  }

}
