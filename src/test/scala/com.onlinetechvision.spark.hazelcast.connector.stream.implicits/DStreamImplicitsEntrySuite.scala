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
import java.util.concurrent.CountDownLatch

import com.hazelcast.core._
import com.hazelcast.map.listener._
import com.onlinetechvision.spark.hazelcast.connector.{SparkHazelcastSuite, DistributedObjectType, SparkHazelcastSuiteUtils}
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastService
import DistributedObjectType.DistributedObjectType
import SparkHazelcastSuiteUtils._
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.Queue

/**
 * Created by eren.avsarogullari on 2/18/16.
 */
@RunWith(classOf[JUnitRunner])
class DStreamImplicitsEntrySuite extends SparkHazelcastSuite {

  before {
    startStreamingContext()
  }

  after {
    stopStreamingContext()
  }

  test("A RDD having 10 integer,string pairs should be written to Hazelcast as Distributed Map with 10 items") {
    test(DistributedObjectType.IMap, "test_distributed_list3", 10)
  }

  test("A RDD having 12 integer,string pairs should be written to Hazelcast as MultiMap with 12 items") {
    test(DistributedObjectType.MultiMap, "test_distributed_set3", 12)
  }

  test("A RDD having 14 integer,string pairs should be written to Hazelcast as ReplicatedMap with 14 items") {
    test(DistributedObjectType.ReplicatedMap, "test_distributed_queue3", 14)
  }

  private def test(distributedObjectType: DistributedObjectType, distributedObjectName: String, expectedItemCount: Int) {
    val properties = getProperties(distributedObjectType, distributedObjectName)

    val expectedTupleList = getExpectedTupleList(expectedItemCount)

    val latch = new CountDownLatch(expectedTupleList.size)
    registerListener[Int, String](properties, latch, expectedTupleList)

    val rddQueue = createRDDQueue[Int, String](expectedTupleList)

    val queueStream = ssc.queueStream(rddQueue)

    queueStream.writeEntryToHazelcast(properties)

    ssc.start()

    latch.await()
  }

  private def createRDDQueue[K, V](expectedTupleList: List[(K, V)]): Queue[RDD[(K, V)]] = {
    val rddQueue: Queue[RDD[(K, V)]] = Queue()
    val tempExpectedBuffer = expectedTupleList.toBuffer
    for (i <- 1 to (expectedTupleList.size / 2)) {
      val intRDD = sc.parallelize[(K, V)](tempExpectedBuffer.take(2))
      rddQueue += intRDD
      tempExpectedBuffer.remove(0, 2)
    }
    rddQueue
  }

  private def registerListener[K, V](properties: Properties, latch: CountDownLatch, expectedTupleList: List[(K, V)]) {
    val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(properties)
    sparkHazelcastData.getDistributedObject() match {
      case hzMap: IMap[K @unchecked, V @unchecked] => {
        hzMap.addEntryListener(new TestMapListener(latch, expectedTupleList), true)
      }
      case multiMap: MultiMap[K @unchecked, V @unchecked] => {
        multiMap.addEntryListener(new TestEntryListener[K, V](latch, expectedTupleList), true)
      }
      case replicatedMap: ReplicatedMap[K @unchecked, V @unchecked] => {
        replicatedMap.addEntryListener(new TestEntryListener[K, V](latch, expectedTupleList))
      }
      case distObj: Any => fail(s"Expected Distributed Object Types : [IMap, MultiMap, ReplicatedMap] but $distObj found!")
    }
  }

  private class TestMapListener[K, V](latch: CountDownLatch, expectedTupleList: List[(K, V)]) extends EntryAddedListener[K, V] with EntryRemovedListener[K, V]
  with EntryUpdatedListener[K, V] with EntryEvictedListener[K, V] {

    override def entryAdded(event: EntryEvent[K, V]) {
      assert(expectedTupleList.contains((event.getKey, event.getValue)))
      latch.countDown()
      println(s"${(event.getKey, event.getValue)} ${latch.getCount}")
    }

    override def entryRemoved(event: EntryEvent[K, V]) {

    }

    override def entryUpdated(event: EntryEvent[K, V]) {

    }

    override def entryEvicted(event: EntryEvent[K, V]) {

    }

  }

  private class TestEntryListener[K, V](latch: CountDownLatch, expectedTupleList: List[(K, V)]) extends EntryListener[K, V] {

    override def entryAdded(event: EntryEvent[K, V]) {
      assert(expectedTupleList.contains((event.getKey, event.getValue)))
      latch.countDown()
      println(s"${(event.getKey, event.getValue)} ${latch.getCount}")
    }

    override def entryRemoved(event: EntryEvent[K, V]) {

    }

    override def entryUpdated(event: EntryEvent[K, V]) {

    }

    override def entryEvicted(event: EntryEvent[K, V]) {

    }

    override def mapCleared(event: MapEvent) {

    }

    override def mapEvicted(event: MapEvent) {

    }

  }

}
