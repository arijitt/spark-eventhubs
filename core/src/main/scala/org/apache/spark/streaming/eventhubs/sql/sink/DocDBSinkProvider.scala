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

package org.apache.spark.streaming.eventhubs.sql.sink

import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.SQLContext

class DocDBSinkProvider extends StreamSinkProvider with DataSourceRegister {

  private def initParameters(parameters: Map[String, String]) = {
    require(parameters.contains("docDB.endpoint") && parameters.contains("docDB.masterKey") &&
      parameters.contains("docDB.databaseId") && parameters.contains("docDB.collectionId") &&
    parameters.contains("docDB.keyColumn") && parameters.contains("docDB.procedureId"))
    (parameters("docDB.endPoint"), parameters("docDB.masterKey"),
      parameters("docDB.databaseId"), parameters("docDB.collectionId"),
      parameters("docDB.procedureId"), parameters("docDB.keyColumn"))
  }

  def shortName(): String = "docDB"

  override def createSink(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    partitionColumns: Seq[String],
    outputMode: OutputMode): Sink = {

    val (endPoint, masterKey, databaseId, collectionId, storedProcedureId, keyColumn) =
      initParameters(parameters)
    new DocDBSink(endPoint, masterKey, databaseId, collectionId, storedProcedureId, keyColumn)
  }
}
