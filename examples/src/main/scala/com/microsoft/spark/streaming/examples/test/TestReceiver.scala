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

import java.io.IOException

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

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
      val futureList = for (partitionId <- 0 until 32) yield
        Future {
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
            while (receivedBuffer.length < rate) {
              cnt += 1
              if (cnt > rate * 2) {
                throw new Exception(s"tried for $cnt times for receiver $receiverId partition" +
                  s" $partitionId")
              }
              receivedBuffer ++= receiver.receive(rate - receivedBuffer.length)
            }
          } catch {
            case e: Exception =>
              e.printStackTrace()
          } finally {
            receiver.close()
          }
        }
      Future.sequence(futureList).onComplete {
        case Failure(e) =>
          e match {
            case ioe: IOException =>
              ioe.printStackTrace()
              sys.exit(1)
            case _ =>
              println(s"finish $receiverId")
          }
        case Success(e) =>
          println(s"finish $receiverId")
      }
      Thread.sleep(interval)
    }
  }
}
