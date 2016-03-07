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

package com.onlinetechvision.spark.hazelcast.connector.validator

import java.util.Properties

import com.hazelcast.config.ClasspathXmlConfig
import com.hazelcast.core.{Hazelcast, HazelcastInstance}
import com.onlinetechvision.spark.hazelcast.connector.DistributedEventType
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastConfig._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by eren.avsarogullari on 3/5/16.
  */
@RunWith(classOf[JUnitRunner])
class SparkHazelcastValidatorSuite extends FunSuite with BeforeAndAfterAll {

  var hazelcastInstance: HazelcastInstance = _

  override def beforeAll() {
    hazelcastInstance = Hazelcast.getOrCreateHazelcastInstance(new ClasspathXmlConfig("hazelcast_test_config.xml"))
  }

  override def afterAll() {
    hazelcastInstance.shutdown()
  }

  test("Validate 'hazelcast.xml.config.file.name' property when it is set as empty.") {
    val properties = new Properties()
    properties.put(HazelcastXMLConfigFileName, "")

    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateProperties(properties)
    }

    assert(ex.getMessage == "'hazelcast.xml.config.file.name' property can not be blank.")
  }

  test("Validate 'hazelcast.xml.config.file.name' property when it is set as non-existent file.") {
    val properties = new Properties()
    properties.put(HazelcastXMLConfigFileName, "nonexistent_hazelcast_test_config.xml")

    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateProperties(properties)
    }

    assert(ex.getMessage == "Specified resource 'nonexistent_hazelcast_test_config.xml' could not be found!")
  }

  test("Validate properties when 'hazelcast.distributed.object.name' property is set as empty.") {
    val properties = new Properties()
    properties.put(HazelcastXMLConfigFileName, "hazelcast_test_config.xml")
    properties.put(HazelcastDistributedObjectName, "")

    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateProperties(properties)
    }

    assert(ex.getMessage == "'hazelcast.distributed.object.name' property can not be blank.")
  }

  test("Validate properties when 'hazelcast.distributed.object.type' property is set as empty.") {
    val properties = new Properties()
    properties.put(HazelcastXMLConfigFileName, "hazelcast_test_config.xml")
    properties.put(HazelcastDistributedObjectName, "test_distributed_map")
    properties.put(HazelcastDistributedObjectType, "")

    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateProperties(properties)
    }

    assert(ex.getMessage == "'hazelcast.distributed.object.type' property must be instanceOf DistributedObjectType")
  }

  test("Validate partition count when 'hazelcast.distributed.object.type' property is set as empty.") {
    val properties = new Properties()
    properties.put(HazelcastXMLConfigFileName, "hazelcast_test_config.xml")
    properties.put(HazelcastDistributedObjectName, "test_distributed_map")
    properties.put(HazelcastDistributedObjectType, "")

    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateProperties(properties)
    }

    assert(ex.getMessage == "'hazelcast.distributed.object.type' property must be instanceOf DistributedObjectType")
  }

  test("Validate partition count when partition count is higher than data count.") {
    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validatePartitionCount(2, 1)
    }

    assert(ex.getMessage == "Partition count must not be higher than Data count.")
  }

  test("Validate DistributedEventTypes for Map Structure when distributed object type is not matched.") {
    val ex = intercept[IllegalStateException] {
      SparkHazelcastValidator.validateDistributedEventTypesOfMap(hazelcastInstance.getList("test_hz_distributed_list"), Set(DistributedEventType.UPDATED))
    }

    assert(ex.getMessage == "Expected Distributed Object Types : [IMap, MultiMap and ReplicatedMap] but test_hz_distributed_list found!")
  }

  test("Validate DistributedEventTypes for Map Structure when event set is empty.") {
    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateDistributedEventTypesOfMap(hazelcastInstance.getMap("test_hz_distributed_map"), Set())
    }

    assert(ex.getMessage == "'distributedEventTypes' can not be empty. Supported values: [ADDED, REMOVED, UPDATED and EVICTED]")
  }

  test("Validate DistributedEventTypes for Map Structure when event set contains null element.") {
    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateDistributedEventTypesOfMap(hazelcastInstance.getMap("test_hz_distributed_map"), Set(DistributedEventType.UPDATED, null))
    }

    assert(ex.getMessage == "'distributedEventTypes' can not contain null element.")
  }

  test("Validate DistributedEventTypes for Distributed List/Set/Queue when distributed object type is not matched.") {
    val ex = intercept[IllegalStateException] {
      SparkHazelcastValidator.validateDistributedEventTypes(hazelcastInstance.getMap("test_hz_distributed_map"), Set(DistributedEventType.ADDED))
    }

    assert(ex.getMessage == "Expected Distributed Object Types : [IList, ISet and IQueue] but test_hz_distributed_map found!")
  }

  test("Validate DistributedEventTypes for Distributed List/Set/Queue when event set is empty.") {
    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateDistributedEventTypes(hazelcastInstance.getList("test_hz_distributed_list"), Set())
    }

    assert(ex.getMessage == "'distributedEventTypes' can not be empty. Supported values: [ADDED and REMOVED]")
  }

  test("Validate DistributedEventTypes for Distributed List/Set/Queue when event set contains null element.") {
    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateDistributedEventTypes(hazelcastInstance.getList("test_hz_distributed_list"), Set(DistributedEventType.ADDED, null))
    }

    assert(ex.getMessage == "'distributedEventTypes' can not contain null element.")
  }

  test("Validate DistributedEventTypes for Distributed List/Set/Queue when event type is not valid.") {
    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateDistributedEventTypes(hazelcastInstance.getList("test_hz_distributed_list"), Set(DistributedEventType.UPDATED))
    }

    assert(ex.getMessage == "Expected Distributed Event Types : Set(ADDED, REMOVED) but UPDATED found!")
  }

}
