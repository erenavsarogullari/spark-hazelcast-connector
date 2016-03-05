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
import java.util.concurrent.CountDownLatch

import com.hazelcast.core._
import com.onlinetechvision.spark.hazelcast.connector.{SparkHazelcastSuite, DistributedObjectType, SparkHazelcastSuiteUtils}
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastService
import SparkHazelcastSuiteUtils._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Created by eren.avsarogullari on 2/17/16.
 */

@RunWith(classOf[JUnitRunner])
class RDDImplicitsSuite extends SparkHazelcastSuite {

  before {
    startSparkContext()
  }

  after {
    stopSparkContext()
  }

  test("A RDD having 100 integer,string pairs should be written to Hazelcast as Distributed Map with 100 items") {
    val expectedList = getExpectedTupleList(100)

    val intRDD = sc.parallelize[(Int, String)](expectedList)

    val properties = getProperties(DistributedObjectType.IMap, "test_distributed_map1")

    import com.onlinetechvision.spark.hazelcast.connector.rdd.implicits._
    intRDD.writeEntryToHazelcast(properties)

    verifyEntries[Int, String](properties, expectedList)
  }

  test("A RDD having 110 integer,string pairs should be written to Hazelcast as MultiMap with 110 items") {
    val expectedList = getExpectedTupleList(110)

    val intRDD = sc.parallelize[(Int, String)](expectedList)

    val properties = getProperties(DistributedObjectType.MultiMap, "test_multi_map1")

    import com.onlinetechvision.spark.hazelcast.connector.rdd.implicits._
    intRDD.writeEntryToHazelcast(properties)

    verifyEntries[Int, String](properties, expectedList)
  }

  test("A RDD having 120 integer,string pairs should be written to Hazelcast as MultiMap with 120 items") {
    val expectedList = getExpectedTupleList(120)

    val intRDD = sc.parallelize[(Int, String)](expectedList)

    val properties = getProperties(DistributedObjectType.ReplicatedMap, "test_replicated_map1")

    import com.onlinetechvision.spark.hazelcast.connector.rdd.implicits._
    intRDD.writeEntryToHazelcast(properties)

    verifyEntries[Int, String](properties, expectedList)
  }

  test("A RDD having 100 integers should be written to Hazelcast as Distributed List with 100 items") {
    val expectedList = getExpectedList(100)
    val intRDD = sc.parallelize[Int](expectedList)

    val properties = getProperties(DistributedObjectType.IList, "test_distributed_list1")

    import com.onlinetechvision.spark.hazelcast.connector.rdd.implicits._
    intRDD.writeItemToHazelcast(properties)

    verifyItems[Int](properties, expectedList)
  }

  test("A RDD having 110 integers should be written to Hazelcast as Distributed Set with 110 items") {
    val expectedList = getExpectedList(110)
    val intRDD = sc.parallelize[Int](expectedList)

    val properties = getProperties(DistributedObjectType.ISet, "test_distributed_set1")

    import com.onlinetechvision.spark.hazelcast.connector.rdd.implicits._
    intRDD.writeItemToHazelcast(properties)

    verifyItems[Int](properties, expectedList)
  }

  test("A RDD having 120 integers should be written to Hazelcast as Distributed Queue with 120 items") {
    val expectedList = getExpectedList(120)
    val intRDD = sc.parallelize[Int](expectedList)

    val properties = getProperties(DistributedObjectType.IQueue, "test_distributed_queue1")

    import com.onlinetechvision.spark.hazelcast.connector.rdd.implicits._
    intRDD.writeItemToHazelcast(properties)

    verifyItems[Int](properties, expectedList)
  }

  test("A RDD having 100 integers should be written to Hazelcast as Topic with 100 items") {
    val expectedList = getExpectedList(100)
    val intRDD = sc.parallelize[Int](expectedList)

    val properties = getProperties(DistributedObjectType.ITopic, "test_topic1")

    val latch = new CountDownLatch(expectedList.size)

    registerMessageListener[Int](properties, new TestMessageListener[Int](expectedList, latch))

    import com.onlinetechvision.spark.hazelcast.connector.rdd.implicits._
    intRDD.writeMessageToHazelcast(properties)

    latch.await()
  }

  test("A RDD having 110 integers should be written to Hazelcast as Reliable Topic with 110 items") {
    val expectedList = getExpectedList(110)
    val intRDD = sc.parallelize[Int](expectedList)

    val properties = getProperties(DistributedObjectType.ReliableTopic, "test_reliable_topic1")

    val latch = new CountDownLatch(expectedList.size)

    registerMessageListener[Int](properties, new TestMessageListener[Int](expectedList, latch))

    import com.onlinetechvision.spark.hazelcast.connector.rdd.implicits._
    intRDD.writeMessageToHazelcast(properties)

    latch.await()
  }

//  test("Unexpected distributed object has to be rejected when entries are written to Hazelcast.") {
//    val expectedList = getExpectedTupleList(100)
//
//    val intRDD = sc.parallelize[(Int, String)](expectedList)
//
//    val properties = getProperties(DistributedObjectType.IList, "unexpected_distributed_object")
//
//    import com.otv.spark.hazelcast.connector.com.onlinetechvision.spark.hazelcast.connector.com.onlinetechvision.spark.hazelcast.connector.rdd.com.onlinetechvision.spark.hazelcast.connector.rdd.com.onlinetechvision.spark.hazelcast.connector.stream.implicits._
//    val ex = intercept[IllegalStateException] {
//      intRDD.writeEntryToHazelcast(properties)
//    }
//
//    (ex.getMessage == "Unexpected Distributed Object Type Found!")
//  }

//  test("Unexpected distributed object has to be rejected when items are written to Hazelcast.") {
//    val expectedList = getExpectedList(100)
//    val intRDD = sc.parallelize[Int](expectedList)
//
//    val properties = getProperties(DistributedObjectType.IMap, "unexpected_distributed_object")
//
//    import com.otv.spark.hazelcast.connector.com.onlinetechvision.spark.hazelcast.connector.com.onlinetechvision.spark.hazelcast.connector.rdd.com.onlinetechvision.spark.hazelcast.connector.rdd.com.onlinetechvision.spark.hazelcast.connector.stream.implicits._
//    val ex = intercept[IllegalStateException]{
//      intRDD.writeItemToHazelcast(properties)
//    }
//
//    (ex.getMessage == "Unexpected Distributed Object Type Found!")
//  }

//  test("Unexpected distributed object has to be rejected when messages are written to Hazelcast.") {
//    val expectedList = getExpectedList(100)
//    val intRDD = sc.parallelize[Int](expectedList)
//
//    val properties = getProperties(DistributedObjectType.MultiMap, "unexpected_distributed_object")
//
//    val latch = new CountDownLatch(expectedList.size)
//
//    registerMessageListener[Int](properties, new TestMessageListener[Int](expectedList, latch))
//
//    import com.otv.spark.hazelcast.connector.com.onlinetechvision.spark.hazelcast.connector.com.onlinetechvision.spark.hazelcast.connector.rdd.com.onlinetechvision.spark.hazelcast.connector.rdd.com.onlinetechvision.spark.hazelcast.connector.stream.implicits._
//    intercept[IllegalStateException] {
//      intRDD.writeMessageToHazelcast(properties)
//    }
// //   latch.await()
//  }

  private def verifyEntries[K, V](properties: Properties, expectedList: List[(K, V)]) {
    import scala.collection.JavaConversions._
    val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(properties)
    sparkHazelcastData.getDistributedObject() match {
      case hzMap: IMap[K @unchecked, V @unchecked] => {
        assert(hzMap.size === expectedList.size)
        for (entry <- hzMap.entrySet()) assert(expectedList.contains((entry.getKey, entry.getValue)))
      }
      case hzMultiMap: MultiMap[K @unchecked, V @unchecked] => {
        assert(hzMultiMap.size === expectedList.size)
        for (entry <- hzMultiMap.entrySet()) assert(expectedList.contains((entry.getKey, entry.getValue)))
      }
      case hzReplicatedMap: ReplicatedMap[K @unchecked, V @unchecked] => {
        assert(hzReplicatedMap.size === expectedList.size)
        for (entry <- hzReplicatedMap.entrySet()) assert(expectedList.contains((entry.getKey, entry.getValue)))
      }
      case distObj: Any => fail(s"Expected Distributed Object Types : [IMap, MultiMap and ReplicatedMap] but ${distObj.getName} found!")
    }
  }

  private def verifyItems[T](properties: Properties, expectedList: List[T]) {
    import scala.collection.JavaConversions._
    val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(properties)
    sparkHazelcastData.getDistributedObject() match {
      case hzList: IList[T @unchecked] => {
        assert(hzList.size === expectedList.size)
        for (item <- hzList) assert(expectedList.contains(item))
      }
      case hzSet: ISet[T @unchecked] => {
        assert(hzSet.size === expectedList.size)
        for (item <- hzSet) assert(expectedList.contains(item))
      }
      case hzQueue: IQueue[T @unchecked] => {
        assert(hzQueue.size === expectedList.size)
        for (item <- hzQueue) assert(expectedList.contains(item))
      }
      case distObj: Any => fail(s"Expected Distributed Object Types : [IList, ISet and IQueue] but ${distObj.getName} found!")
    }
  }

  private def registerMessageListener[T](properties: Properties, listener: MessageListener[T]) {
    val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(properties)
    sparkHazelcastData.getDistributedObject() match {
      case hzTopic: ITopic[T @unchecked] => {
        hzTopic.addMessageListener(listener)
      }
      case distObj: Any => fail(s"Expected Distributed Object Types : [ITopic and ReliableTopic] but ${distObj.getName} found!")
    }
  }

  private class TestMessageListener[T](expectedList: List[T], latch: CountDownLatch) extends MessageListener[T] {
    override def onMessage(message: Message[T]) {
      assert(expectedList.contains(message.getMessageObject))
      latch.countDown()
    }
  }

}
