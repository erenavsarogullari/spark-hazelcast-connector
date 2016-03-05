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
import com.onlinetechvision.spark.hazelcast.connector.{SparkHazelcastSuite, DistributedObjectType, SparkHazelcastSuiteUtils}
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastService
import DistributedObjectType.DistributedObjectType
import SparkHazelcastSuiteUtils._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Created by eren.avsarogullari on 2/18/16.
 */
@RunWith(classOf[JUnitRunner])
class DStreamImplicitsItemSuite extends SparkHazelcastSuite {

  before {
    startStreamingContext()
  }

  after {
    stopStreamingContext()
  }

  test("A RDD having 10 integers should be written to Hazelcast as Distributed List with 10 items") {
    test(DistributedObjectType.IList, "test_distributed_list3", 10)
  }

  test("A RDD having 12 integers should be written to Hazelcast as Distributed Set with 12 items") {
    test(DistributedObjectType.ISet, "test_distributed_set3", 12)
  }

  test("A RDD having 14 integers should be written to Hazelcast as Distributed Queue with 14 items") {
    test(DistributedObjectType.IQueue, "test_distributed_queue3", 14)
  }

  private def test(distributedObjectType: DistributedObjectType, distributedObjectName: String, expectedItemCount: Int) {
    val properties = getProperties(distributedObjectType, distributedObjectName)

    val expectedList = getExpectedList(expectedItemCount)

    val latch = new CountDownLatch(expectedList.size)
    registerListener[Int](properties, new TestItemListener[Int](latch, expectedList))

    val rddQueue = createRDDQueue[Int](sc, expectedList)

    val queueStream = ssc.queueStream(rddQueue)

    queueStream.writeItemToHazelcast(properties)

    ssc.start()

    latch.await()
  }

  private def registerListener[T](properties: Properties, listener: ItemListener[T]) {
    val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(properties)
    sparkHazelcastData.getDistributedObject() match {
      case hzList: IList[T @unchecked] => {
        hzList.addItemListener(listener, true)
      }
      case hzSet: ISet[T @unchecked] => {
        hzSet.addItemListener(listener, true)
      }
      case hzQueue: IQueue[T @unchecked] => {
        hzQueue.addItemListener(listener, true)
      }
      case distObj: Any => fail(s"Expected Distributed Object Types : [IList, ISet, IQueue] but ${distObj.getName} found!")
    }
  }

  private class TestItemListener[T](latch: CountDownLatch, expectedList: List[T]) extends ItemListener[T] {

    override def itemAdded(item: ItemEvent[T]) {
      assert(expectedList.contains(item.getItem))
      latch.countDown()
      println(s"${item.getItem} ${latch.getCount}")
    }

    override def itemRemoved(item: ItemEvent[T]) {

    }

  }

}
