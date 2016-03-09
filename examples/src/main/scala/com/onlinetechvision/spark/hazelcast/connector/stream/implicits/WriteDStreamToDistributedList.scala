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

package com.onlinetechvision.spark.hazelcast.connector.stream.implicits

import java.util.Properties

import com.hazelcast.config.ClasspathXmlConfig
import com.hazelcast.core.{Hazelcast, IList}
import com.onlinetechvision.spark.hazelcast.connector.{User, DistributedObjectType}
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastConfig
import SparkHazelcastConfig._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Queue

/**
 * Created by eren.avsarogullari on 2/9/16.
 */
object WriteDStreamToDistributedList {

  val HazelcastXMLFileName = "hazelcast_config.xml"
  val HazelcastDistributedListName = "test_distributed_list"

  def main(args: Array[String]) {

    // Hazelcast Distributed Object Content Printer is started...
    new Thread(new HazelcastDistributedObjectContentPrinter).start()

    // Spark Context is created...
    val sc = new SparkContext(new SparkConf().setAppName("spark-hazelcast").setMaster("local[2]"))
    val ssc = new StreamingContext(sc, Seconds(3))

    // Spark Hazelcast properties are created...
    val sparkHazelcastProperties = new Properties()
    sparkHazelcastProperties.put(HazelcastXMLConfigFileName, HazelcastXMLFileName)
    sparkHazelcastProperties.put(HazelcastDistributedObjectName, HazelcastDistributedListName)
    sparkHazelcastProperties.put(HazelcastDistributedObjectType, DistributedObjectType.IList)

    // RDD Queue is created for queue stream
    val rddQueue: Queue[RDD[User]] = Queue()
    (1 to 100).foreach(index => {
      val data = Array(new User(index, s"name$index", s"surname$index"))
      val userRDD = sc.parallelize[User](data)
      rddQueue += userRDD
    })

    // User Stream is created...
    val userStream = ssc.queueStream(rddQueue)

    // User Stream is written to Hazelcast as Distributed List...
    import com.onlinetechvision.spark.hazelcast.connector.stream.implicits._
    userStream.writeItemToHazelcast(sparkHazelcastProperties)

    // Spark Streaming Context is started...
    ssc.start()

  }

  class HazelcastDistributedObjectContentPrinter extends Runnable {

    override def run(): Unit = {
      // Distributed List is created to print the item count...
      val hzInstance = Hazelcast.getOrCreateHazelcastInstance(new ClasspathXmlConfig(HazelcastXMLFileName))
      val distributedList: IList[Int] = hzInstance.getList(HazelcastDistributedListName)
      (1 to 1000).foreach(index => {
        Thread.sleep(3000)
        println(s"'test_distributed_list' has got ${distributedList.size} elements.")
      })
    }

  }

}
