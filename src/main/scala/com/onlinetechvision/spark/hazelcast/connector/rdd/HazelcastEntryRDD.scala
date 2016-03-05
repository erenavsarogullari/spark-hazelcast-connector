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

import java.util.Map.Entry
import java.util.Properties

import com.hazelcast.core.{MultiMap, _}
import com.onlinetechvision.spark.hazelcast.connector.validator.SparkHazelcastValidator
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

class HazelcastEntryRDD[K, V](@transient private val sc: SparkContext,
                              properties: Properties, partitions: Int = 3) extends RDD[Entry[K, V]](sc, Nil) with HazelcastRDD {

  override val props: Properties = properties



  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Entry[K, V]] = {
    val hazelcastTuple = getHazelcastTuple(sparkHazelcastData.getDistributedObject())
    val hazelcastIterator = hazelcastTuple._1
    val entryCount = hazelcastTuple._2

    SparkHazelcastValidator.validatePartitionCount(partitions, entryCount)

    val indexTuple = getIndexTuple(split.index, entryCount, partitions)
    println(s"index : ${split.index}, from : ${indexTuple._1}, to : ${indexTuple._2}")
    hazelcastIterator.slice(indexTuple._1, indexTuple._2)
  }

  override protected def getPartitions: Array[Partition] = getPartitions(partitions)

  private def getHazelcastTuple(distributedObject: DistributedObject): (Iterator[Entry[K, V]], Int) = {
    distributedObject match {
      case hzMap: IMap[K, V] => (new HazelcastIterator[Entry[K, V]](hzMap.entrySet().iterator()), hzMap.entrySet().size())
      case hzMultiMap: MultiMap[K, V] => (new HazelcastIterator[Entry[K, V]](hzMultiMap.entrySet().iterator()), hzMultiMap.entrySet().size())
      case hzReplicatedMap: ReplicatedMap[K, V] => (new HazelcastIterator[Entry[K, V]](hzReplicatedMap.entrySet().iterator()), hzReplicatedMap.entrySet().size())
      case distObj: Any => throw new IllegalStateException(s"Expected Distributed Object Types : [IMap, MultiMap and ReplicatedMap] but ${distObj.getName} found!")
    }
  }

}
