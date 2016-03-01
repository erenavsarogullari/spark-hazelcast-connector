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
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastService
import org.apache.spark.Partition

/**
 * Created by eren.avsarogullari on 2/8/16.
 */
trait HazelcastRDD {

  protected val props: Properties

  protected lazy val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(props)

  protected def getIndexTuple(index: Int, entryCount: Int, partitions: Int): (Int, Int) ={
    val entryCountPerPartition = Math.round((entryCount.toDouble / partitions.toDouble)).toInt
    val from = index * entryCountPerPartition
    val to = getEndIndex(index, entryCount, partitions, entryCountPerPartition)
    (from, to)
  }

  protected def getPartitions(partitions: Int): Array[Partition] = {
    (0 to partitions - 1).map(p => new HazelcastPartition(p)).toArray
  }

  private def getEndIndex(index: Int, entryCount: Int, partitions: Int, entryCountPerPartition: Int): Int = {
    if(index < (partitions - 1)) {
      (index + 1) * entryCountPerPartition
    } else {
      entryCount
    }
  }

  protected class HazelcastIterator[T](hzIterator: java.util.Iterator[T]) extends Iterator[T] with Serializable {
    override def hasNext: Boolean = hzIterator.hasNext

    override def next() = hzIterator.next()
  }

  protected class HazelcastPartition(idx: Int) extends Partition {
    override def index: Int = idx

    override def toString = s"HazelcastPartition($idx)"
  }

}
