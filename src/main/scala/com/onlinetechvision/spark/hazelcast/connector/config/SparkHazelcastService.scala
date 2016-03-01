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

package com.onlinetechvision.spark.hazelcast.connector.config

import java.util.Properties

import com.hazelcast.config.ClasspathXmlConfig
import com.hazelcast.core._
import com.onlinetechvision.spark.hazelcast.connector.DistributedObjectType
import DistributedObjectType._
import org.apache.commons.lang3.Validate

/**
 * Created by eren.avsarogullari on 2/8/16.
 */
object SparkHazelcastService {

  def getSparkHazelcastData(properties: Properties): SparkHazelcastData = {
    validateProperties(properties)

    val hazelcastXMLConfigFileName = properties.getProperty(SparkHazelcastConfig.HazelcastXMLConfigFileName)
    val hazelcastDistributedObjectName = properties.getProperty(SparkHazelcastConfig.HazelcastDistributedObjectName)
    val hazelcastDistributedObjectType = properties.get(SparkHazelcastConfig.HazelcastDistributedObjectType).asInstanceOf[DistributedObjectType]

    val hazelcastInstance = Hazelcast.getOrCreateHazelcastInstance(new ClasspathXmlConfig(hazelcastXMLConfigFileName))
    val distributedObject = getDistributedObject(hazelcastInstance, hazelcastDistributedObjectType, hazelcastDistributedObjectName)
    new SparkHazelcastData(distributedObject)
  }

  private def validateProperties(properties: Properties): Unit = {
    Validate.notBlank(properties.getProperty(SparkHazelcastConfig.HazelcastXMLConfigFileName))
    Validate.notBlank(properties.getProperty(SparkHazelcastConfig.HazelcastDistributedObjectName))
    Validate.notNull(properties.get(SparkHazelcastConfig.HazelcastDistributedObjectType))
  }

  private def getDistributedObject(hazelcastInstance: HazelcastInstance, hazelcastDistributedObjectType: DistributedObjectType, hazelcastDistributedObjectName: String): DistributedObject = {
    hazelcastDistributedObjectType match {
      case DistributedObjectType.IMap => {
        hazelcastInstance.getMap(hazelcastDistributedObjectName)
      }
      case DistributedObjectType.MultiMap => {
        hazelcastInstance.getMultiMap(hazelcastDistributedObjectName)
      }
      case DistributedObjectType.ReplicatedMap => {
        hazelcastInstance.getReplicatedMap(hazelcastDistributedObjectName)
      }
      case DistributedObjectType.IList => {
        hazelcastInstance.getList(hazelcastDistributedObjectName)
      }
      case DistributedObjectType.ISet => {
        hazelcastInstance.getSet(hazelcastDistributedObjectName)
      }
      case DistributedObjectType.IQueue => {
        hazelcastInstance.getQueue(hazelcastDistributedObjectName)
      }
      case DistributedObjectType.ITopic => {
        hazelcastInstance.getTopic(hazelcastDistributedObjectName)
      }
      case DistributedObjectType.ReliableTopic => {
        hazelcastInstance.getReliableTopic(hazelcastDistributedObjectName)
      }
      case distObj: Any => throw new IllegalStateException(s"Expected Distributed Object Types : [IMap, MultiMap, ReplicatedMap, IList, ISet and IQueue, ITopic and ReliableTopic] but $distObj found!")
    }
  }

  //  private def getHazelcastDistributedEventTypes(hazelcastDistributedEventTypes: String): Set[DistributedEventType] = {
  //    if(StringUtils.isNotBlank(hazelcastDistributedEventTypes)) {
  //      val distributedEventTypeSet = Set[DistributedEventType]()
  //      val types = StringUtils.split(hazelcastDistributedEventTypes, ",")
  //      types.foreach(t => {
  //        val tenum = DistributedEventType.withName(t)
  //        distributedEventTypeSet += tenum
  //      })
  //      distributedEventTypeSet
  //    } else {
  //      Set()
  //    }
  //  }

}
