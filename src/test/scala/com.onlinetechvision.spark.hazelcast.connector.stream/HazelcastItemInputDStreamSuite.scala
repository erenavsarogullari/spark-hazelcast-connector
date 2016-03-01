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

import com.hazelcast.core.{IList, ISet, _}
import com.onlinetechvision.spark.hazelcast.connector.DistributedObjectType._
import com.onlinetechvision.spark.hazelcast.connector.SparkHazelcastSuiteUtils._
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastService
import com.onlinetechvision.spark.hazelcast.connector.{DistributedObjectType, SparkHazelcastSuite, SparkHazelcastSuiteUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.storage.StorageLevel
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Created by eren.avsarogullari on 2/9/16.
 */
@RunWith(classOf[JUnitRunner])
class HazelcastItemInputDStreamSuite extends SparkHazelcastSuite {

  before {
    startStreamingContext()
  }

  after {
    stopStreamingContext()
  }

  test("A Distributed List having 10 integers should be written to Spark as DStream") {
    test(DistributedObjectType.IList, "test_distributed_list4", 10)
  }

  test("A Distributed Set having 15 integers should be written to Spark as DStream") {
    test(DistributedObjectType.ISet, "test_distributed_set4", 15)
  }

  test("A Distributed Queue having 20 integers should be written to Spark as DStream") {
    test(DistributedObjectType.IQueue, "test_distributed_queue4", 20)
  }

  private def test(distributedObjectType: DistributedObjectType, distributedObjectName: String, expectedMessageCount: Int) {
    val expectedList = getExpectedList(expectedMessageCount)

    val properties = getProperties(distributedObjectType, distributedObjectName)

    new Thread(new HazelcastItemLoader[Int](properties, expectedList)).start()

    val latch = new CountDownLatch(expectedList.size)

    val hazelcastItemStream = HazelcastUtils.createHazelcastItemStream[Int](ssc, StorageLevel.MEMORY_ONLY, properties)

    verify[Int](hazelcastItemStream, expectedList, latch)

    ssc.start()

    latch.await()
  }

  private def verify[T](hazelcastItemStream: HazelcastItemInputDStream[T], expectedList: List[T], latch: CountDownLatch): Unit = {
    hazelcastItemStream.foreachRDD(rdd => {
      rdd.collect().foreach(tuple => {
        assert(StringUtils.isNotBlank(tuple._1))
        assert(StringUtils.isNotBlank(tuple._2))
        assert(expectedList.contains(tuple._3))
      })
      latch.countDown()
    })
  }

  class HazelcastItemLoader[T](properties: Properties, expectedList: List[T]) extends Runnable {

    override def run(): Unit = {
      val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(properties)
      sparkHazelcastData.getDistributedObject() match {
        case hzList: IList[T] => {
          addItems(hzList, expectedList)
        }
        case hzSet: ISet[T] => {
          addItems(hzSet, expectedList)
        }
        case hzQueue: IQueue[T] => {
          addItems(hzQueue, expectedList)
        }
        case distObj: Any => fail(s"Expected Distributed Object Types : [IList, ISet and IQueue] but $distObj found!")
      }
    }

    private def addItems(hzCollection: ICollection[T], expectedList: List[T]): Unit = {
      expectedList.foreach(item => {
        hzCollection.add(item)
        Thread.sleep(1000)
      })
    }

  }

}
