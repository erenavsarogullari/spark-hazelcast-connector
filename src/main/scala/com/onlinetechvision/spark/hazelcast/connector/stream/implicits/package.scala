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

import com.onlinetechvision.spark.hazelcast.connector.rdd.implicits._
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by eren.avsarogullari on 2/9/16.
 */
package object implicits {

  implicit class HazelcastDStreamEntryWriter[K,V](dStream: DStream[(K,V)]) {

    def writeEntryToHazelcast(properties: Properties): Unit = {
      dStream.foreachRDD(rdd => rdd.writeEntryToHazelcast(properties))
    }
  }

  implicit class HazelcastDStreamItemWriter[T](dStream: DStream[T]) {

    def writeItemToHazelcast(properties: Properties): Unit = {
      dStream.foreachRDD(rdd => rdd.writeItemToHazelcast(properties))
    }

  }

  implicit class HazelcastDStreamMessageWriter[T](dStream: DStream[T]) {

    def writeMessageToHazelcast(properties: Properties): Unit = {
      dStream.foreachRDD(rdd => rdd.writeMessageToHazelcast(properties))
    }

  }

}
