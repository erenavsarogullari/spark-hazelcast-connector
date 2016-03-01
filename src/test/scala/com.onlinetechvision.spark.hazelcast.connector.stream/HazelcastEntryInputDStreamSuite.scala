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

package com.otv.spark.hazelcast.connector.stream

import java.util.Properties
import java.util.concurrent.CountDownLatch

import com.hazelcast.core._
import com.onlinetechvision.spark.hazelcast.connector.stream.{HazelcastEntryInputDStream, HazelcastUtils}
import com.onlinetechvision.spark.hazelcast.connector.{SparkHazelcastSuite, DistributedObjectType, DistributedEventType, SparkHazelcastSuiteUtils}
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastService
import DistributedEventType._
import DistributedObjectType._
import SparkHazelcastSuiteUtils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.storage.StorageLevel
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Created by eren.avsarogullari on 2/9/16.
 */
@RunWith(classOf[JUnitRunner])
class HazelcastEntryInputDStreamSuite extends SparkHazelcastSuite {

  before {
    startStreamingContext()
  }

  after {
    stopStreamingContext()
  }

  test("A Distributed List having 10 integers should be written to Spark as DStream") {
    test(DistributedObjectType.IMap, "test_distributed_map4", 10)
  }

  test("A Distributed Set having 15 integers should be written to Spark as DStream") {
    test(DistributedObjectType.MultiMap, "test_distributed_multiMap4", 15)
  }

  test("A Distributed Queue having 20 integers should be written to Spark as DStream") {
    test(DistributedObjectType.ReplicatedMap, "test_distributed_replicatedMap4", 20)
  }

  private def test(distributedObjectType: DistributedObjectType, distributedObjectName: String, expectedMessageCount: Int, distributedEventTypes: Set[DistributedEventType] = Set(DistributedEventType.ADDED, DistributedEventType.REMOVED, DistributedEventType.UPDATED)) {
    val expectedTupleList = getExpectedTupleList(expectedMessageCount)

    val properties = getProperties(distributedObjectType, distributedObjectName)

    new Thread(new HazelcastEntryLoader[Int,String](properties, expectedTupleList)).start()

    val latch = new CountDownLatch(expectedTupleList.size)

    val hazelcastEntryStream = HazelcastUtils.createHazelcastEntryStream[Int,String](ssc, StorageLevel.MEMORY_ONLY, properties)

    verify[Int,String](hazelcastEntryStream, expectedTupleList, latch)

    ssc.start()

    latch.await()
  }

  private def verify[K,V](hazelcastEntryStream: HazelcastEntryInputDStream[K,V], expectedTupleList: List[(K,V)], latch: CountDownLatch, distributedEventTypes: Set[DistributedEventType] = Set(DistributedEventType.ADDED)) {
    hazelcastEntryStream.foreachRDD(rdd => {
      rdd.collect().foreach(tuple => {
        assert(StringUtils.isNotBlank(tuple._1))
        assert(StringUtils.isNotBlank(tuple._2))
        assert(distributedEventTypes.contains(DistributedEventType.withName(tuple._2)))
        assert(expectedTupleList.contains((tuple._3, tuple._5)))
      })
      latch.countDown()
    })
  }

  class HazelcastEntryLoader[Int,String](properties: Properties, expectedTupleList: List[(Int,String)]) extends Runnable {

    override def run(): Unit = {
      val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(properties)
      sparkHazelcastData.getDistributedObject() match {
        case hzMap: IMap[Int,String] => {
          expectedTupleList.foreach(tuple => {
            hzMap.put(tuple._1, tuple._2)
            hzMap.put(tuple._1, tuple._2)
            hzMap.remove(tuple._1)
            Thread.sleep(1000)
          })
        }
        case multiMap: MultiMap[Int,String] => {
          expectedTupleList.foreach(tuple => {
            multiMap.put(tuple._1, tuple._2)
            multiMap.remove(tuple._1)
            Thread.sleep(1000)
          })
        }
        case replicatedMap: ReplicatedMap[Int,String] => {
          expectedTupleList.foreach(tuple => {
            replicatedMap.put(tuple._1, tuple._2)
            replicatedMap.put(tuple._1, tuple._2)
            replicatedMap.remove(tuple._1)
            Thread.sleep(1000)
          })
        }
        case distObj: Any => fail(s"Expected Distributed Object Types : [IMap, MultiMap and ReplicatedMap] but $distObj found!")
      }
    }

  }

}
