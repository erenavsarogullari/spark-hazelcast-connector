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
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class HazelcastItemRDD[I: ClassTag](@transient val sc: SparkContext,
                                    val properties: Properties, partitions: Int = 3) extends RDD[I](sc, Nil) with HazelcastRDD {

  override protected val props: Properties = properties

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[I] = {
    val hazelcastTuple = getHazelcastTuple(sparkHazelcastData.getDistributedObject())
    val hazelcastIterator = hazelcastTuple._1
    val entryCount = hazelcastTuple._2
    val indexTuple = getIndexTuple(split.index, entryCount, partitions)
    hazelcastIterator.slice(indexTuple._1, indexTuple._2)
  }

  override protected def getPartitions: Array[Partition] = getPartitions(partitions)

  private def getHazelcastTuple(distributedObject: DistributedObject): (Iterator[I], Int) = {
    distributedObject match {
      case hzList: IList[I] => (new HazelcastIterator[I](hzList.iterator()), hzList.size())
      case hzSet: ISet[I] => (new HazelcastIterator[I](hzSet.iterator()), hzSet.size())
      case hzQueue: IQueue[I] => (new HazelcastIterator[I](hzQueue.iterator()), hzQueue.size())
      case distObj: Any => throw new IllegalStateException(s"Expected Distributed Object Types : [IList, ISet and IQueue] but $distObj found!")
    }
  }

}
