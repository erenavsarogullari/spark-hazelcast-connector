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
import com.onlinetechvision.spark.hazelcast.connector.DistributedEventType
import DistributedEventType._
import com.onlinetechvision.spark.hazelcast.connector.validator.SparkHazelcastValidator
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

/**
 * Created by eren.avsarogullari on 2/12/16.
 */
class HazelcastItemInputDStream[T](ssc: StreamingContext, storageLevel: StorageLevel, properties: Properties, distributedEventTypes: Set[DistributedEventType] = Set(DistributedEventType.ADDED)) extends ReceiverInputDStream[(String, String, T)](ssc) {
  override def getReceiver(): Receiver[(String, String, T)] = new HazelcastItemReceiver[T](storageLevel, properties, distributedEventTypes)
}

class HazelcastItemReceiver[T](storageLevel: StorageLevel, properties: Properties, distributedEventTypes: Set[DistributedEventType] = Set(DistributedEventType.ADDED)) extends Receiver[(String, String, T)](storageLevel) with HazelcastReceiver {

  override protected val props: Properties = properties

  SparkHazelcastValidator.validateDistributedEventTypes[T](sparkHazelcastData.getDistributedObject(), distributedEventTypes)

  override def onStart() {
    start()
  }

  override def onStop() {
    stop()
  }

  override protected def registerListener(distributedObject: DistributedObject): String = {
    val hazelcastInputDStreamItemListener = new HazelcastInputDStreamItemListener[T](this, distributedEventTypes)
    distributedObject match {
      case hzList: IList[T] => hzList.addItemListener(hazelcastInputDStreamItemListener, true)
      case hzSet: ISet[T] => hzSet.addItemListener(hazelcastInputDStreamItemListener, true)
      case hzQueue: IQueue[T] => hzQueue.addItemListener(hazelcastInputDStreamItemListener, true)
      case distObj: Any => throw new IllegalStateException(s"Expected Distributed Object Types : [IList, ISet and IQueue] but ${distObj.getName} found!")
    }
  }

  override protected def unregisterListener(distributedObject: DistributedObject, registrationId: String) {
    distributedObject match {
      case hzList: IList[T] => hzList.removeItemListener(registrationId)
      case hzSet: ISet[T] => hzSet.removeItemListener(registrationId)
      case hzQueue: IQueue[T] => hzQueue.removeItemListener(registrationId)
    }
  }

  private class HazelcastInputDStreamItemListener[T](receiver: HazelcastItemReceiver[T], distributedEventTypes: Set[DistributedEventType]) extends ItemListener[T] {

    override def itemAdded(item: ItemEvent[T]) {
      store(item)
    }

    override def itemRemoved(item: ItemEvent[T]) {
      store(item)
    }

    private def store(item: ItemEvent[T]) {
      if (distributedEventTypes.contains(DistributedEventType.withName(item.getEventType.name()))) {
        receiver.store((item.getMember.getAddress.toString, item.getEventType.name(), item.getItem))
      }
    }

  }

}


