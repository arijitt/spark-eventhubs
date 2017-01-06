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

import com.microsoft.azure.eventhubs.{EventData, EventHubClient}
import com.microsoft.azure.servicebus.ConnectionStringBuilder

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

    import scala.collection.JavaConverters._

    var offset = new Array[Long](32)

    for (receiverId <- 0 until totalReceiverNum) {
      val futureList = for (partitionId <- 0 until 32) yield
        Future {
          val startOffset = offset(partitionId)
          println(s"starting receiver $receiverId partition $partitionId with the start offset" +
            s" $startOffset")

          val connectionString = new ConnectionStringBuilder(namespace, name, policyName,
            policyKey)
          val client = EventHubClient.createFromConnectionString(connectionString.toString).get()
          val receiver = client.createReceiver("$Default", partitionId.toString,
            startOffset.toString).get()

          val receivedBuffer = new ListBuffer[EventData]
          var cnt = 0
          try {
            while (receivedBuffer.length < rate) {
              cnt += 1
              if (cnt > rate * 2) {
                throw new Exception(s"tried for $cnt times for receiver $receiverId partition" +
                  s" $partitionId")
              }
              receivedBuffer ++= receiver.receiveSync(rate - receivedBuffer.length).asScala
            }
            receivedBuffer.last.getSystemProperties.getOffset
          } catch {
            case e: Exception =>
              e.printStackTrace()
          } finally {
            receiver.closeSync()
            client.closeSync()
            receivedBuffer.last.getSystemProperties.getOffset
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
          offset = e.map(_.toString.toLong).toArray
          println(s"finish ${receiverId} with offset ${offset.toList}")
      }
      Thread.sleep(interval)
    }
  }
}
