/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.onlinetechvision.spark.hazelcast.connector.rdd.implicits

import java.util.Properties

import com.hazelcast.core.{Hazelcast, IMap}
import com.onlinetechvision.spark.hazelcast.connector.{User, DistributedObjectType}
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastConfig._
import org.apache.spark.{SparkConf, SparkContext}

object WriteRDDToDistributedMap {

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
    val userRDD = sc.parallelize[User](data).map(user => (user.id, s"${user.name} ${user.surname}"))

    // Number of elements in RDD is printed as 5...
    println(s"userRDD has got ${userRDD.count} elements.")

    // Spark Hazelcast properties are created...
    val sparkHazelcastProperties = new Properties()
    sparkHazelcastProperties.put(HazelcastXMLConfigFileName, "hazelcast_config.xml")
    sparkHazelcastProperties.put(HazelcastDistributedObjectName, "test_distributed_map")
    sparkHazelcastProperties.put(HazelcastDistributedObjectType, DistributedObjectType.IMap)

    // userRDD is written to Hazelcast as a Distributed Map...
    import com.onlinetechvision.spark.hazelcast.connector.rdd.implicits._
    userRDD.writeEntryToHazelcast(sparkHazelcastProperties)

    // Gets 'test_distributed_map' Hazelcast Distributed Map instance...
    val hzInstance = Hazelcast.getHazelcastInstanceByName("test_hazelcast_instance")
    val hzDistributedMap: IMap[Int, String] = hzInstance.getMap("test_distributed_map")

    // Prints items of 'test_distributed_map'
    import scala.collection.JavaConversions.asScalaIterator
    for (entry <- hzDistributedMap.entrySet().iterator()) {
      println(entry)
    }

  }

}
