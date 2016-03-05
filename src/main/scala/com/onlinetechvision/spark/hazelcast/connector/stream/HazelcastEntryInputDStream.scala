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

import com.hazelcast.core._
import com.hazelcast.map.listener._
import com.hazelcast.query.Predicate
import com.onlinetechvision.spark.hazelcast.connector.DistributedEventType
import DistributedEventType._
import com.onlinetechvision.spark.hazelcast.connector.validator.SparkHazelcastValidator
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

/**
 * Created by eren.avsarogullari on 2/14/16.
 */
class HazelcastEntryInputDStream[K, V](ssc: StreamingContext, storageLevel: StorageLevel, properties: Properties, distributedEventTypes: Set[DistributedEventType] = Set(DistributedEventType.ADDED), predicate: Option[Predicate[K,V]] = None, key: Option[K] = None) extends ReceiverInputDStream[(String, String, K, V, V)](ssc) {
  override def getReceiver(): Receiver[(String, String, K, V, V)] = new HazelcastEntryReceiver[K, V](storageLevel, properties, distributedEventTypes, predicate, key)
}

class HazelcastEntryReceiver[K, V](storageLevel: StorageLevel, properties: Properties, distributedEventTypes: Set[DistributedEventType] = Set(DistributedEventType.ADDED), predicate: Option[Predicate[K,V]] = None, key: Option[K] = None) extends Receiver[(String, String, K, V, V)](storageLevel) with HazelcastReceiver {

  override protected val props: Properties = properties

  SparkHazelcastValidator.validateDistributedEventTypesOfMap[K, V](sparkHazelcastData.getDistributedObject(), distributedEventTypes)

  override def onStart() {
    start()
  }

  override def onStop() {
    stop()
  }

  override protected def registerListener(distributedObject: DistributedObject): String = {
    distributedObject match {
      case hzMap: IMap[K, V] => {
        val listener = new HazelcastInputDStreamMapListener[K, V](this, distributedEventTypes)
        if(!predicate.isEmpty && key.isEmpty) {
          hzMap.addEntryListener(listener, predicate.get, true)
        } else if(predicate.isEmpty && !key.isEmpty) {
          hzMap.addEntryListener(listener, key.get, true)
        } else if(!predicate.isEmpty && !key.isEmpty) {
          hzMap.addEntryListener(listener, predicate.get, key.get, true)
        } else {
          hzMap.addEntryListener(listener, true)
        }
      }
      case hzMultiMap: MultiMap[K, V] => {
        val listener = new HazelcastInputDStreamEntryListener[K, V](this, distributedEventTypes)
        if(!key.isEmpty) {
          hzMultiMap.addEntryListener(listener, key.get, true)
        } else {
          hzMultiMap.addEntryListener(listener, true)
        }
      }
      case hzReplicatedMap: ReplicatedMap[K, V] => {
        val listener = new HazelcastInputDStreamEntryListener[K, V](this, distributedEventTypes)
        if(!predicate.isEmpty && key.isEmpty) {
          hzReplicatedMap.addEntryListener(listener, predicate.get)
        } else if(predicate.isEmpty && !key.isEmpty) {
          hzReplicatedMap.addEntryListener(listener, key.get)
        } else if(!predicate.isEmpty && !key.isEmpty) {
          hzReplicatedMap.addEntryListener(listener, predicate.get, key.get)
        } else {
          hzReplicatedMap.addEntryListener(listener)
        }
      }

      case distObj: Any => throw new IllegalStateException(s"Expected Distributed Object Types : [IMap, MultiMap and ReplicatedMap] but ${distObj.getName} found!")
    }
  }

  override protected def unregisterListener(distributedObject: DistributedObject, registrationId: String) {
    distributedObject match {
      case hzMap: IMap[K, V] => hzMap.removeEntryListener(listenerRegistrationId)
      case hzMultiMap: MultiMap[K, V] => hzMultiMap.removeEntryListener(listenerRegistrationId)
      case hzReplicatedMap: ReplicatedMap[K, V] => hzReplicatedMap.removeEntryListener(listenerRegistrationId)
    }
  }

  private class HazelcastInputDStreamMapListener[K, V](receiver: HazelcastEntryReceiver[K, V], distributedEventTypes: Set[DistributedEventType])
                                                extends EntryAddedListener[K, V] with EntryRemovedListener[K, V] with EntryUpdatedListener[K, V] with EntryEvictedListener[K, V] {

    override def entryAdded(event: EntryEvent[K, V]) {
      store(event)
    }

    override def entryRemoved(event: EntryEvent[K, V]) {
      store(event)
    }

    override def entryUpdated(event: EntryEvent[K, V]) {
      store(event)
    }

    override def entryEvicted(event: EntryEvent[K, V]) {
      store(event)
    }

    private def store(event: EntryEvent[K, V]) {
      if(distributedEventTypes.contains(DistributedEventType.withName(event.getEventType.name()))) {
        receiver.store((event.getMember.getAddress.toString, event.getEventType.name(), event.getKey, event.getOldValue, event.getValue))
      }
    }

  }

  private class HazelcastInputDStreamEntryListener[K, V](receiver: HazelcastEntryReceiver[K, V], distributedEventTypes: Set[DistributedEventType]) extends EntryListener[K, V] {

    override def entryAdded(event: EntryEvent[K, V]) {
      store(event)
    }

    override def entryRemoved(event: EntryEvent[K, V]) {
      store(event)
    }

    override def entryUpdated(event: EntryEvent[K, V]) {
      store(event)
    }

    override def entryEvicted(event: EntryEvent[K, V]) {
      store(event)
    }

    override def mapEvicted(event: MapEvent): Unit = ???

    override def mapCleared(event: MapEvent): Unit = ???

    private def store(event: EntryEvent[K, V]) {
      if(distributedEventTypes.contains(DistributedEventType.withName(event.getEventType.name()))) {
        receiver.store((event.getMember.getAddress.toString, event.getEventType.name(), event.getKey, event.getOldValue, event.getValue))
      }
    }

  }

}
