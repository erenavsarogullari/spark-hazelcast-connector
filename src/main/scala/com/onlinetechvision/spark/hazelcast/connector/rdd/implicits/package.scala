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
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastService
import org.apache.spark.rdd.RDD

/**
 * Created by eren.avsarogullari on 2/8/16.
 */
package object implicits {

  protected trait HazelcastWriter[T] extends Serializable {

    protected val rdd: RDD[T]

    protected def write(iterator: Iterator[T], distributedObject: DistributedObject)

    protected def writeToHazelcast(properties: Properties) {
      rdd.sparkContext.runJob(rdd, ((iterator: Iterator[T]) => new HazelcastTask(properties).execute(iterator)))
    }

    private class HazelcastTask(properties: Properties) extends Serializable {
      def execute(iterator: Iterator[T]) {
        val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(properties)
        write(iterator, sparkHazelcastData.getDistributedObject())
      }
    }

  }

  implicit class HazelcastItemWriter[T](receivedRDD: RDD[T]) extends HazelcastWriter[T] {

    override val rdd: RDD[T] = receivedRDD

    override protected def write(iterator: Iterator[T], distributedObject: DistributedObject) {
      distributedObject match {
        case hzList: IList[T] => iterator.foreach(item => hzList.add(item))
        case hzSet: ISet[T] => iterator.foreach(item => hzSet.add(item))
        case hzQueue: IQueue[T] => iterator.foreach(item => hzQueue.add(item))
        case _ => throw new IllegalStateException("Unexpected Distributed Object Type Found!")
      }
    }

    def writeItemToHazelcast(properties: Properties) {
      writeToHazelcast(properties)
    }

  }

  implicit class HazelcastMessageWriter[T](receivedRDD: RDD[T]) extends HazelcastWriter[T] {

    override val rdd: RDD[T] = receivedRDD

    override protected def write(iterator: Iterator[T], distributedObject: DistributedObject) {
      distributedObject match {
        case hzTopic: ITopic[T] => iterator.foreach(message => hzTopic.publish(message))
        case _ => throw new IllegalStateException("Unexpected Distributed Object Type Found!")
      }
    }

    def writeMessageToHazelcast(properties: Properties) {
      writeToHazelcast(properties)
    }

  }

  implicit class HazelcastEntryWriter[K, V](receivedRDD: RDD[(K, V)]) extends HazelcastWriter[(K, V)] {

    override val rdd: RDD[(K, V)] = receivedRDD

    override protected def write(iterator: Iterator[(K, V)], distributedObject: DistributedObject) {
      distributedObject match {
        case hzMap: IMap[K, V] => iterator.foreach(tuple => hzMap.put(tuple._1, tuple._2))
        case hzMultiMap: MultiMap[K, V] => iterator.foreach(tuple => hzMultiMap.put(tuple._1, tuple._2))
        case hzReplicatedMap: ReplicatedMap[K, V] => iterator.foreach(tuple => hzReplicatedMap.put(tuple._1, tuple._2))
        case _ => throw new IllegalStateException("Unexpected Distributed Object Type Found!")
      }
    }

    def writeEntryToHazelcast(properties: Properties) {
      writeToHazelcast(properties)
    }

  }

}
