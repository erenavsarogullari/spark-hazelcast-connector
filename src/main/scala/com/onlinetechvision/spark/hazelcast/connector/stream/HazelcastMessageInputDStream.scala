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
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

/**
 * Created by eren.avsarogullari on 2/16/16.
 */
class HazelcastMessageInputDStream[T](ssc: StreamingContext, storageLevel: StorageLevel, properties: Properties) extends ReceiverInputDStream[(String, String, T)](ssc) {
  override def getReceiver(): Receiver[(String, String, T)] = new HazelcastMessageReceiver[T](storageLevel, properties)
}

class HazelcastMessageReceiver[T](storageLevel: StorageLevel, properties: Properties) extends Receiver[(String, String, T)](storageLevel) with HazelcastReceiver {

  override protected val props: Properties = properties

  override def onStart() {
    start()
  }

  override def onStop() = {
    stop()
  }

  override protected def registerListener(distributedObject: DistributedObject): String = {
    val hazelcastInputDStreamMessageListener = new HazelcastInputDStreamMessageListener[T](this)
    distributedObject match {
      case hzTopic: ITopic[T] => hzTopic.addMessageListener(hazelcastInputDStreamMessageListener)
      case distObj: Any => throw new IllegalStateException(s"Expected Distributed Object Type : [ITopic] but $distObj found!")
    }
  }

  override protected def unregisterListener(distributedObject: DistributedObject, registrationId: String) {
    distributedObject match {
      case hzTopic: ITopic[T] => hzTopic.removeMessageListener(registrationId)
    }
  }

  private class HazelcastInputDStreamMessageListener[T](receiver: HazelcastMessageReceiver[T]) extends MessageListener[T] {

    override def onMessage(message: Message[T]) {
      println(s"geldi.... ${message.getMessageObject}")
      receiver.store((message.getPublishingMember.getAddress.toString, message.getPublishTime.toString, message.getMessageObject))
    }

  }

}
