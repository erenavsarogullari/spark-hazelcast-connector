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

import com.hazelcast.core.DistributedObject
import com.onlinetechvision.spark.hazelcast.connector.config.SparkHazelcastService

/**
 * Created by eren.avsarogullari on 2/14/16.
 */
trait HazelcastReceiver {

  protected val props: Properties

  @transient protected lazy val sparkHazelcastData = SparkHazelcastService.getSparkHazelcastData(props)

  protected var listenerRegistrationId = ""

  protected def start() {
    listenerRegistrationId = registerListener(sparkHazelcastData.getDistributedObject())
  }

  protected def stop() = unregisterListener(sparkHazelcastData.getDistributedObject(), listenerRegistrationId)

  protected def registerListener(distributedObject: DistributedObject): String

  protected def unregisterListener(distributedObject: DistributedObject, registrationId: String)

}
