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

package com.microsoft.spark.streaming.examples.sql

import scala.io.Source

import com.microsoft.azure.documentdb._

object DocumentDBFeeder {


  def main(args: Array[String]): Unit = {

    val END_POINT = args(0)
    val MASTER_KEY = args(1)
    val DATABASE_ID = args(2)
    val TEMP_COLLECTION_ID = args(3)
    val MAIN_COLLECTION_ID = args(5)

    val docDBClient = new DocumentClient(END_POINT, MASTER_KEY, ConnectionPolicy.GetDefault(),
      ConsistencyLevel.BoundedStaleness)

    val requestOptions = new RequestOptions()
    requestOptions.setOfferThroughput(1000)

    val stream = getClass.getResourceAsStream("/bulkimport.js")
    val bulkImport = Source.fromInputStream(stream).getLines().foldLeft("")(
      (str, line) => str + line + "\n")

    println(bulkImport)

    val remoteProcedure = docDBClient.queryStoredProcedures(
      "dbs/" + DATABASE_ID + "/colls/" + TEMP_COLLECTION_ID,
      new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
        new SqlParameterCollection(new SqlParameter(
          "@id", "prod-1"))), null).getQueryIterable.toList.get(0)

    docDBClient.deleteStoredProcedure(remoteProcedure.getSelfLink, null)

    docDBClient.deleteCollection("dbs/" + DATABASE_ID + "/colls/" + TEMP_COLLECTION_ID, null)

    val tempDocCollection = new DocumentCollection()
    tempDocCollection.setId(TEMP_COLLECTION_ID)
    docDBClient.createCollection("dbs/" + DATABASE_ID, tempDocCollection, null)

    val newProcedure = new StoredProcedure()
    newProcedure.setId("prod-1")
    newProcedure.setBody(bulkImport)

    val newlyCreatedProcedure = docDBClient.createStoredProcedure(
      "dbs/" + DATABASE_ID + "/colls/" + TEMP_COLLECTION_ID, newProcedure, null).getResource

    val docListBuffer = new Array[Object](args(4).toInt)

    val batchId = System.currentTimeMillis()

    for (i <- 0 until args(4).toInt) {
      val allenDocument = new Document(s"""{\"name\":\"John\",\"batchId\":\"$batchId\"}""")
      allenDocument.setId(i.toString)
      docListBuffer(i) = allenDocument
    }

    val k = docListBuffer.map(doc => doc.asInstanceOf[JsonSerializable].toJson())

    // Array.fill[Object](k.length + 1)
    val parameters = new Array[Object](k.length + 1)
    for (id <- k.indices) {
      parameters(id) = k
    }
    parameters(parameters.length - 1) = true.asInstanceOf[AnyRef]

    docDBClient.executeStoredProcedure(newlyCreatedProcedure.getSelfLink, parameters)

    val mainCollections = docDBClient.queryCollections("dbs/" + DATABASE_ID,
      new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
        new SqlParameterCollection(new SqlParameter(
          "@id", MAIN_COLLECTION_ID))),
      null).getQueryIterable.toList

    if (mainCollections.size() > 0) {
      mainCollections.get(0)
    } else {
      val documentCollection = new DocumentCollection()
      documentCollection.setId(MAIN_COLLECTION_ID)
      docDBClient.createCollection("dbs/" + DATABASE_ID, documentCollection, null).getResource
    }


  }
}
