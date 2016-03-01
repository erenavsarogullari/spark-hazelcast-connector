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

import com.hazelcast.query.Predicate
import com.onlinetechvision.spark.hazelcast.connector.DistributedEventType
import com.onlinetechvision.spark.hazelcast.connector.DistributedEventType._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext

/**
 * Created by eren.avsarogullari on 2/10/16.
 */
object HazelcastUtils {

  def createHazelcastEntryStream[K, V](ssc: StreamingContext, storageLevel: StorageLevel, properties: Properties, distributedEventTypes: Set[DistributedEventType] = Set(DistributedEventType.ADDED), predicate: Option[Predicate[K, V]] = None, key: Option[K] = None): HazelcastEntryInputDStream[K, V] = {
    new HazelcastEntryInputDStream[K, V](ssc, storageLevel, properties, distributedEventTypes, predicate, key)
  }

  def createHazelcastItemStream[T](ssc: StreamingContext, storageLevel: StorageLevel, properties: Properties, distributedEventTypes: Set[DistributedEventType] = Set(DistributedEventType.ADDED)): HazelcastItemInputDStream[T] = {
    new HazelcastItemInputDStream[T](ssc, storageLevel, properties, distributedEventTypes)
  }

  def createHazelcastMessageStream[T](ssc: StreamingContext, storageLevel: StorageLevel, properties: Properties): HazelcastMessageInputDStream[T] = {
    new HazelcastMessageInputDStream[T](ssc, storageLevel, properties)
  }

}
