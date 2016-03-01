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
class DStreamImplicitsMessageSuite extends SparkHazelcastSuite {

  before {
    startStreamingContext()
  }

  after {
    stopStreamingContext()
  }

  test("A RDD having 10 integers should be written to Hazelcast as Topic with 10 messages") {
    test(DistributedObjectType.ITopic, "test_topic3", 10)
  }

  test("A RDD having 12 integers should be written to Hazelcast as Reliable Topic with 12 messages") {
    test(DistributedObjectType.ReliableTopic, "test_reliableTopic3", 12)
  }

  private def test(distributedObjectType: DistributedObjectType, distributedObjectName: String, expectedItemCount: Int) {
    val properties = getProperties(distributedObjectType, distributedObjectName)

    val expectedList = getExpectedList(expectedItemCount)

    val latch = new CountDownLatch(expectedList.size)
    registerListener[Int](properties, new TestMessageListener[Int](latch, expectedList))

    val rddQueue = createRDDQueue[Int](sc, expectedList)

    val queueStream = ssc.queueStream(rddQueue)

    queueStream.writeMessageToHazelcast(properties)

    ssc.start()

    latch.await()
  }

  private def registerListener[T](properties: Properties, listener: MessageListener[T]) {
    val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(properties)
    sparkHazelcastData.getDistributedObject() match {
      case hzTopic: ITopic[T @unchecked] => {
        hzTopic.addMessageListener(listener)
      }
      case distObj: Any => fail(s"Expected Distributed Object Type : [ITopic] but $distObj found!")
    }
  }

  private class TestMessageListener[T](latch: CountDownLatch, expectedList: List[T]) extends MessageListener[T] {

    override def onMessage(message: Message[T]) {
      assert(expectedList.contains(message.getMessageObject))
      latch.countDown()
      println(s"${message.getMessageObject} ${latch.getCount}")
    }
  }

}
