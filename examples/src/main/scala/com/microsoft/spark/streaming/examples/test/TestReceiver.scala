/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.spark.streaming.examples.test

import scala.collection.mutable.ListBuffer

import com.microsoft.azure.eventhubs.EventData

import org.apache.spark.streaming.eventhubs.EventHubsClientWrapper

object TestReceiver {

  def main(args: Array[String]): Unit = {
    val totalReceiverNum = args(0).toInt
    val rate = args(1).toInt
    val namespace = args(2)
    val name = args(3)
    val policyName = args(4)
    val policyKey = args(5)
    val interval = args(6).toInt

    for (receiverId <- 0 until totalReceiverNum) {
      for (partitionId <- 0 until 32) {
        val startOffset: Long = receiverId * rate
        println(s"starting receiver $receiverId partition $partitionId with the start offset" +
          s" $startOffset")
        val receiver = EventHubsClientWrapper.getEventHubClient(
            Map(
              "eventhubs.namespace" -> namespace,
              "eventhubs.name" -> name,
              "eventhubs.policyname" -> policyName,
              "eventhubs.policykey" -> policyKey,
              "eventhubs.consumergroup" -> "$Default"),
            partitionId,
            startOffset,
            rate)
        val receivedBuffer = new ListBuffer[EventData]
        var cnt = 0
        try {
          while (receivedBuffer.length < rate && cnt < rate) {
            cnt += 1
            receivedBuffer ++= receiver.receive(rate - receivedBuffer.length)
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
        } finally {
          receiver.close()
        }
      }
      Thread.sleep(interval)
    }
  }
}
