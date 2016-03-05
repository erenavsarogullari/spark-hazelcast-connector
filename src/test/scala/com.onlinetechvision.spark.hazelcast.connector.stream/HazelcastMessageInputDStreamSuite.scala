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

package com.onlinetechvision.spark.hazelcast.connector.stream

import java.util.Properties
import java.util.concurrent.CountDownLatch

import com.hazelcast.core.ITopic
import com.onlinetechvision.spark.hazelcast.connector.DistributedObjectType._
import com.onlinetechvision.spark.hazelcast.connector.SparkHazelcastSuiteUtils._
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastService
import com.onlinetechvision.spark.hazelcast.connector.{DistributedObjectType, SparkHazelcastSuite}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.storage.StorageLevel
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Created by eren.avsarogullari on 2/20/16.
 */
@RunWith(classOf[JUnitRunner])
class HazelcastMessageInputDStreamSuite extends SparkHazelcastSuite {

  before {
    startStreamingContext()
  }

  after {
    stopStreamingContext()
  }

  test("A Topic having 20 integers should be written to Spark as DStream") {
    test(DistributedObjectType.ITopic, "test_topic4", 20)
  }

  test("A Reliable Topic having 30 integers should be written to Spark as DStream") {
    test(DistributedObjectType.ReliableTopic, "test_reliableTopic4", 30)
  }

  private def test(distributedObjectType: DistributedObjectType, distributedObjectName: String, expectedMessageCount: Int) {
    val expectedList = getExpectedList(expectedMessageCount)

    val properties = getProperties(distributedObjectType, distributedObjectName)

    new Thread(new HazelcastMessageLoader[Int](properties, expectedList)).start()

    val latch = new CountDownLatch(expectedList.size)

    val hazelcastMessageStream = HazelcastUtils.createHazelcastMessageStream[Int](ssc, StorageLevel.MEMORY_ONLY, properties)

    verify[Int](hazelcastMessageStream, expectedList, latch)

    ssc.start()

    latch.await()
  }

  private def verify[T](hazelcastMessageStream: HazelcastMessageInputDStream[T], expectedList: List[T], latch: CountDownLatch): Unit = {
    hazelcastMessageStream.foreachRDD(rdd => {
      rdd.collect().foreach(tuple => {
        assert(StringUtils.isNotBlank(tuple._1))
        assert(StringUtils.isNotBlank(tuple._2))
        assert(expectedList.contains(tuple._3))
      })
      latch.countDown()
    })
  }

  class HazelcastMessageLoader[T](properties: Properties, expectedList: List[T]) extends Runnable {

    override def run(): Unit = {
      val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(properties)
      sparkHazelcastData.getDistributedObject() match {
        case hzTopic: ITopic[T] => {
          expectedList.foreach(message => {
            hzTopic.publish(message)
            Thread.sleep(1000)
          })
        }
        case distObj: Any => fail(s"Expected Distributed Object Types : [ITopic] but ${distObj.getName} found!")
      }
    }

  }

}
