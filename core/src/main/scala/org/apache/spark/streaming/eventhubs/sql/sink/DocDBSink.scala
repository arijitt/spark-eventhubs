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

import java.util.ServiceLoader

import scala.io.Source

import com.microsoft.azure.documentdb._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.streaming.{ForeachSink, Sink}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.Utils

class DocDBSink(
    endPoint: String,
    masterKey: String,
    databaseId: String,
    collectionId: String,
    storedProcedureId: String,
    keyColumn: String) extends Sink {

  private val collectionLink = "dbs/" + databaseId  + "/colls/" + collectionId
  private val (documentClient: DocumentClient, storedProcedure: StoredProcedure) = initEntities()

  private def loadStoredProcedure(): String = {
    val stream = getClass.getResourceAsStream("/bulkimport.js")
    Source.fromInputStream(stream).getLines().foldLeft("")((str, line) => str + line + "\n")
  }

  private def initEntities(): (DocumentClient, StoredProcedure) = {
    // init client
    val client = new DocumentClient(endPoint, masterKey, ConnectionPolicy.GetDefault(),
      ConsistencyLevel.BoundedStaleness)
    // init procedure
    val remoteProcedures = client.queryStoredProcedures(
      collectionLink,
      new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
        new SqlParameterCollection(new SqlParameter(
          "@id", storedProcedureId))), null).getQueryIterable.toList
    if (remoteProcedures.size() > 0) {
      val procedure = remoteProcedures.get(0)
      client.deleteStoredProcedure(procedure.getSelfLink, null)
    }
    val newProcedure = new StoredProcedure()
    newProcedure.setId(storedProcedureId)
    newProcedure.setBody(loadStoredProcedure())
    val sProc = client.createStoredProcedure(
      collectionLink, newProcedure, null).getResource
    (client, sProc)
  }

  initEntities()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val docCol = data.toJSON.toDF("jsonDoc").col("jsonDoc")
    data.select(keyColumn).withColumn("docCol", docCol).map(row =>
      {
        val doc = new Document(row.getAs[String]("jsonDoc"))
        doc.setId(row.getAs[String](keyColumn))
        doc
      }).foreach(doc => documentClient.replaceDocument(doc, null))
  }

}
