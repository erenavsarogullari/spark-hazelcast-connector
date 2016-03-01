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

package com.onlinetechvision.spark.hazelcast.connector.rdd

import java.util.Properties

import com.hazelcast.core.{IList, IQueue, ISet}
import com.onlinetechvision.spark.hazelcast.connector.SparkHazelcastSuiteUtils._
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastService
import com.onlinetechvision.spark.hazelcast.connector.{DistributedObjectType, SparkHazelcastSuite, SparkHazelcastSuiteUtils}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Created by eren.avsarogullari on 2/18/16.
 */
@RunWith(classOf[JUnitRunner])
class HazelcastItemRDDSuite extends SparkHazelcastSuite {

  before {
    startSparkContext()
  }

  after {
    stopSparkContext()
  }

  test("An Distributed List having 100 integers should be written to Spark as RDD") {
    val expectedList = getExpectedList(100)

    val properties = getProperties(DistributedObjectType.IList, "test_distributed_list2")

    addItems[Int](properties, expectedList)

    val hazelcastItemRDD = new HazelcastItemRDD[Int](sc, properties)

    verify[Int](hazelcastItemRDD, expectedList)
  }

  test("An Distributed Set having 110 integers should be written to Spark as RDD") {
    val expectedList = getExpectedList(110)

    val properties = getProperties(DistributedObjectType.ISet, "test_distributed_set2")

    addItems[Int](properties, expectedList)

    val hazelcastItemRDD = new HazelcastItemRDD[Int](sc, properties)

    verify[Int](hazelcastItemRDD, expectedList)
  }

  test("An Distributed Queue having 120 integers should be written to Spark as RDD") {
    val expectedList = getExpectedList(120)

    val properties = getProperties(DistributedObjectType.IQueue, "test_distributed_queue2")

    addItems[Int](properties, expectedList)

    val hazelcastItemRDD = new HazelcastItemRDD[Int](sc, properties)

    verify[Int](hazelcastItemRDD, expectedList)
  }

  private def addItems[T](properties: Properties, expectedList: List[T]) {
    import scala.collection.JavaConversions._
    val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(properties)
    sparkHazelcastData.getDistributedObject() match {
      case hzList: IList[T@unchecked] => {
        assert(hzList.addAll(expectedList))
      }
      case hzSet: ISet[T@unchecked] => {
        assert(hzSet.addAll(expectedList))
      }
      case hzQueue: IQueue[T@unchecked] => {
        assert(hzQueue.addAll(expectedList))
      }
      case distObj: Any => fail(s"Expected Distributed Object Types : [IList, ISet and IQueue] but $distObj found!")
    }
  }

  private def verify[T](rdd: HazelcastItemRDD[T], expectedList: List[T]): Unit = {
    assert(rdd.count() === expectedList.size)
    rdd.collect().foreach(x => assert(expectedList.contains(x)))
  }

}
