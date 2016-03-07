package com.onlinetechvision.spark.hazelcast.connector.rdd.implicits

import java.util.Properties

import com.hazelcast.core.{Hazelcast, IList}
import com.onlinetechvision.spark.hazelcast.connector.{User, DistributedObjectType}
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastConfig._
import org.apache.spark.{SparkConf, SparkContext}

object WriteRDDToDistributedList {

  def main(args: Array[String]) {

    // Spark Context is created...
    val sc = new SparkContext(new SparkConf().setAppName("spark-hazelcast").setMaster("local"))

    // RDD content is created...
    val data = Array(User(1, "name1", "surname1"),
                      User(2, "name2", "surname2"),
                      User(3, "name3", "surname3"),
                      User(4, "name4", "surname4"),
                      User(5, "name5", "surname5"))

    // RDD is created...
    val userRDD = sc.parallelize[User](data)

    // Number of elements in RDD is printed as 5...
    println(s"userRDD has got ${userRDD.count} elements.")

    // Spark Hazelcast properties are created...
    val sparkHazelcastProperties = new Properties()
    sparkHazelcastProperties.put(HazelcastXMLConfigFileName, "hazelcast_config.xml")
    sparkHazelcastProperties.put(HazelcastDistributedObjectName, "test_distributed_list")
    sparkHazelcastProperties.put(HazelcastDistributedObjectType, DistributedObjectType.IList)

    // userRDD is written to Hazelcast as a Distributed List...
    import com.onlinetechvision.spark.hazelcast.connector.rdd.implicits._
    userRDD.writeItemToHazelcast(sparkHazelcastProperties)

    // Gets 'test_distributed_list' Hazelcast Distributed List instance...
    val hzInstance = Hazelcast.getHazelcastInstanceByName("test_hazelcast_instance")
    val hzDistributedList: IList[User] = hzInstance.getList("test_distributed_list")

    // Prints items of 'test_distributed_list'
    import scala.collection.JavaConversions.asScalaIterator
    for (item <- hzDistributedList.iterator()) {
      println(item)
    }

  }

}
