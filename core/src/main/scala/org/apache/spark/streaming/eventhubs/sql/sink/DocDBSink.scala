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

import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.Utils

class DocDBSink(endPoint: String, masterKey: String,
                databaseId: String, collectionId: String, storedProcedureId: String) extends Sink {

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
    val remoteProcedures = documentClient.queryStoredProcedures(
      collectionLink,
      new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
        new SqlParameterCollection(new SqlParameter(
          "@id", storedProcedureId))), null).getQueryIterable.toList
    if (remoteProcedures.size() > 0) {
      val procedure = remoteProcedures.get(0)
      documentClient.deleteStoredProcedure(procedure.getSelfLink, null)
    }
    val newProcedure = new StoredProcedure()
    newProcedure.setId("spark.streaming.DocDBSinkBulkImport")
    newProcedure.setBody(loadStoredProcedure())
    val sProc = documentClient.createStoredProcedure(
      collectionLink, newProcedure, null).getResource
    (client, sProc)
  }

  initEntities()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    import scala.collection.JavaConverters._
    println(ServiceLoader.load(classOf[DataSourceRegister], Utils.getContextOrSparkClassLoader).
      asScala.toList)
    println(data.collect().toList)
    // step 1: translate DF to json
    // convert every row to json

    // write to DocumentDB atomically
    // data.write.
  }

}
