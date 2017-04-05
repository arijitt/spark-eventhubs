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

package org.apache.spark.sql.streaming

import org.apache.spark.sql.execution.streaming.{MemoryStream, Offset, Source, StreamExecution}

/** A trait for actions that can be performed while testing a streaming DataFrame. */
trait StreamAction

case class EventHubsAddDataMemory[A](source: MemoryStream[A], data: Seq[A])
  extends EventHubsAddData {
  override def toString: String = s"AddData to $source: ${data.mkString(",")}"

  override def addData(query: Option[StreamExecution]): (Source, Offset) = {
    (source, source.addData(data))
  }
}

/**
  * Adds the given data to the stream. Subsequent check answers will block
  * until this data has been processed.
  * */

object EventHubsAddData {
  def apply[A](source: MemoryStream[A], data: A*): EventHubsAddDataMemory[A] =
    EventHubsAddDataMemory(source, data)
}

/** A trait that can be extended when testing a source. */
trait EventHubsAddData extends StreamAction with Serializable {
  /**
    * Called to adding the data to a source. It should find the source to add data to from
    * the active query, and then return the source object the data was added, as well as the
    * offset of added data.
    */
  def addData(query: Option[StreamExecution]): (Source, Offset)
}