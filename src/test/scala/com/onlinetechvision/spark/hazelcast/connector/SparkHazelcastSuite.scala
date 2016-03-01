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

package com.onlinetechvision.spark.hazelcast.connector

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{FunSuite, BeforeAndAfter}

/**
 * Created by eren.avsarogullari on 2/20/16.
 */
trait SparkHazelcastSuite extends FunSuite with BeforeAndAfter {

  private lazy val sparkConf = new SparkConf().setAppName("spark-hazelcast").setMaster("local[2]")
  protected var sc: SparkContext = _
  protected var ssc: StreamingContext = _

  protected def startSparkContext() {
    sc = new SparkContext(sparkConf)
  }

  protected def stopSparkContext() {
    sc.stop()
  }

  protected def startStreamingContext() {
    sc = new SparkContext(sparkConf)
    ssc = new StreamingContext(sc, Seconds(1))
  }

  protected def stopStreamingContext() {
    ssc.stop()
  }

}
