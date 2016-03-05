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

import com.hazelcast.core._
import com.onlinetechvision.spark.hazelcast.connector.{SparkHazelcastSuite, DistributedObjectType, SparkHazelcastSuiteUtils}
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastService
import SparkHazelcastSuiteUtils._
import com.onlinetechvision.spark.hazelcast.connector.SparkHazelcastSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Created by eren.avsarogullari on 2/18/16.
 */
@RunWith(classOf[JUnitRunner])
class HazelcastEntryRDDSuite extends SparkHazelcastSuite {

  before {
    startSparkContext()
  }

  after {
    stopSparkContext()
  }

  test("A Distributed Map having 100 integer,string pairs should be written to Spark as RDD") {
    val properties = getProperties(DistributedObjectType.IMap, "test_distributed_map2")

    val expectedTupleList = getExpectedTupleList(100)

    addEntries[Int, String](properties, expectedTupleList)

    val hazelcastEntryRDD = new HazelcastEntryRDD[Int, String](sc, properties)

    verify[Int, String](hazelcastEntryRDD, expectedTupleList)
  }

  test("A MultiMap having 110 integer,string pairs should be written to Spark as RDD") {
    val properties = getProperties(DistributedObjectType.MultiMap, "test_multiMap2")

    val expectedTupleList = getExpectedTupleList(110)

    addEntries[Int, String](properties, expectedTupleList)

    val hazelcastEntryRDD = new HazelcastEntryRDD[Int, String](sc, properties)

    verify[Int, String](hazelcastEntryRDD, expectedTupleList)
  }

  test("A ReplicatedMap having 120 integer,string pairs should be written to Spark as RDD") {
    val properties = getProperties(DistributedObjectType.ReplicatedMap, "test_replicatedMap2")

    val expectedTupleList = getExpectedTupleList(120)

    addEntries[Int, String](properties, expectedTupleList)

    val hazelcastEntryRDD = new HazelcastEntryRDD[Int, String](sc, properties)

    verify[Int, String](hazelcastEntryRDD, expectedTupleList)
  }

  private def addEntries[K, V](properties: Properties, expectedTupleList: List[(K, V)]) {
    val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(properties)
    sparkHazelcastData.getDistributedObject() match {
      case hzMap: IMap[K @unchecked, V @unchecked] => {
        expectedTupleList.foreach(tuple => hzMap.put(tuple._1, tuple._2))
      }
      case hzMultiMap: MultiMap[K @unchecked, V @unchecked] => {
        expectedTupleList.foreach(tuple => hzMultiMap.put(tuple._1, tuple._2))
      }
      case hzReplicatedMap: ReplicatedMap[K @unchecked, V @unchecked] => {
        expectedTupleList.foreach(tuple => hzReplicatedMap.put(tuple._1, tuple._2))
      }
      case distObj: Any => fail(s"Expected Distributed Object Types : [IMap, MultiMap and ReplicatedMap] but ${distObj.getName} found!")
    }
  }

  private def verify[K, V](rdd: HazelcastEntryRDD[K, V], expectedTupleList: List[(K, V)]) {
    assert(rdd.count() === expectedTupleList.size)
    rdd.collect().foreach(entry => assert(expectedTupleList.contains((entry.getKey, entry.getValue))))
  }

}
