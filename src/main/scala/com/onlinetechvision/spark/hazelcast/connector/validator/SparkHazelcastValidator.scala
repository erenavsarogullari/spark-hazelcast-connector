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
import com.hazelcast.core.IList
import com.hazelcast.core.IMap
import com.hazelcast.core.IQueue
import com.hazelcast.core.ISet
import com.hazelcast.core.MultiMap
import com.hazelcast.core.ReplicatedMap
import com.hazelcast.core._
import com.onlinetechvision.spark.hazelcast.connector.DistributedEventType
import com.onlinetechvision.spark.hazelcast.connector.DistributedEventType._
import com.onlinetechvision.spark.hazelcast.connector.DistributedObjectType._
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastConfig
import org.apache.commons.lang3.Validate

/**
  * Created by eren.avsarogullari on 3/4/16.
  */
object SparkHazelcastValidator {

  val supportedDistributedEventTypesOfMaps: Set[DistributedEventType] = Set(DistributedEventType.ADDED, DistributedEventType.REMOVED, DistributedEventType.UPDATED, DistributedEventType.EVICTED)
  val supportedDistributedEventTypes: Set[DistributedEventType] = Set(DistributedEventType.ADDED, DistributedEventType.REMOVED)

  def validateProperties(properties: Properties) {
    Validate.notBlank(properties.getProperty(SparkHazelcastConfig.HazelcastXMLConfigFileName), "'hazelcast.xml.config.file.name' property can not be blank.")
    Validate.notNull(new ClasspathXmlConfig(properties.getProperty(SparkHazelcastConfig.HazelcastXMLConfigFileName)), "'hazelcast.xml.config.file.name' property can not be null.")
    Validate.notBlank(properties.getProperty(SparkHazelcastConfig.HazelcastDistributedObjectName), "'hazelcast.distributed.object.name' property can not be blank.")
    if(!properties.get(SparkHazelcastConfig.HazelcastDistributedObjectType).isInstanceOf[DistributedObjectType])
      throw new IllegalArgumentException("'hazelcast.distributed.object.type' property must be instanceOf DistributedObjectType")
  }

  def validatePartitionCount(partitions: Int, dataCount: Int) {
    if(partitions > dataCount) throw new IllegalArgumentException("Partition count must not be higher than Data count.")
  }

  def validateDistributedEventTypesOfMap[K,V](distributedObject: DistributedObject, distributedEventTypes: Set[DistributedEventType]) {
    Validate.notEmpty(distributedEventTypes.toArray, "'distributedEventTypes' can not be empty. Supported values: [ADDED, REMOVED, UPDATED and EVICTED]")
    Validate.noNullElements(distributedEventTypes.toArray, "'distributedEventTypes' can not contain null element.")
    distributedObject match {
      case hzMap: IMap[K @unchecked,V @unchecked] => checkDistributedEventTypes(distributedEventTypes, supportedDistributedEventTypesOfMaps)
      case multiMap: MultiMap[K @unchecked,V @unchecked] => checkDistributedEventTypes(distributedEventTypes, supportedDistributedEventTypesOfMaps)
      case replicatedMap: ReplicatedMap[K @unchecked,V @unchecked] => checkDistributedEventTypes(distributedEventTypes, supportedDistributedEventTypesOfMaps)
      case distObj: Any => throw new IllegalStateException(s"Expected Distributed Object Types : [IMap, MultiMap and ReplicatedMap] but ${distObj.getName} found!")
    }
  }

  def validateDistributedEventTypes[T](distributedObject: DistributedObject, distributedEventTypes: Set[DistributedEventType]) {
    Validate.notEmpty(distributedEventTypes.toArray, "'distributedEventTypes' can not be empty. Supported values: [ADDED and REMOVED]")
    Validate.noNullElements(distributedEventTypes.toArray,  "'distributedEventTypes' can not contain null element.")
    distributedObject match {
      case hzList: IList[T @unchecked] => checkDistributedEventTypes(distributedEventTypes, supportedDistributedEventTypes)
      case hzList: ISet[T @unchecked] => checkDistributedEventTypes(distributedEventTypes, supportedDistributedEventTypes)
      case hzList: IQueue[T @unchecked] => checkDistributedEventTypes(distributedEventTypes, supportedDistributedEventTypes)
      case distObj: Any => throw new IllegalStateException(s"Expected Distributed Object Types : [IList, ISet and IQueue] but ${distObj.getName} found!")
    }
  }

  private def checkDistributedEventTypes(distributedEventTypes: Set[DistributedEventType], supportedDistributedEventTypes: Set[DistributedEventType]) {
    distributedEventTypes.foreach(eventType => {
      if(!supportedDistributedEventTypes.contains(eventType))
        throw new IllegalArgumentException(s"Expected Distributed Event Types : $supportedDistributedEventTypes but $eventType found!")
    })
  }

}
