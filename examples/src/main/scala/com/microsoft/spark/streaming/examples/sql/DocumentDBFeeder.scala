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
    val COLLECTION_ID = args(3)

    val docDBClient = new DocumentClient(END_POINT, MASTER_KEY, ConnectionPolicy.GetDefault(),
      ConsistencyLevel.BoundedStaleness)

    val requestOptions = new RequestOptions()
    requestOptions.setOfferThroughput(1000)

    /*
    val myDB = new Database()
    myDB.setId(DATABASE_ID)

    // Create a new database.
    // TODO: try if it throws exception when DB exists
    val newDB = docDBClient.createDatabase(myDB, null).getResource

    var myCollection = new DocumentCollection()
    myCollection.setId(COLLECTION_ID)

    myCollection = docDBClient.createCollection(
      "dbs/" + DATABASE_ID, myCollection, requestOptions).getResource

    System.out.println("Created a new collection:")
    System.out.println(myCollection.toString())
    */

    val stream = getClass.getResourceAsStream("/bulkimport.js")
    val bulkImport = Source.fromInputStream(stream).getLines().foldLeft("")(
      (str, line) => str + line + "\n")

    println(bulkImport)

    val remoteProcedure = docDBClient.queryStoredProcedures(
      "dbs/" + DATABASE_ID + "/colls/" + COLLECTION_ID,
      new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
        new SqlParameterCollection(new SqlParameter(
          "@id", "prod-1"))), null).getQueryIterable.toList.get(0)

    docDBClient.deleteStoredProcedure(remoteProcedure.getSelfLink, null)

    val newProcedure = new StoredProcedure()
    newProcedure.setId("prod-1")
    newProcedure.setBody(bulkImport)


    val newlyCreatedProcedure = docDBClient.createStoredProcedure(
      "dbs/" + DATABASE_ID + "/colls/" + COLLECTION_ID, newProcedure, null).getResource

    val docListBuffer = new Array[Object](args(4).toInt)

    for (i <- 0 until args(4).toInt) {
      val allenDocument = new Document("{\"name\":\"John\"}")
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



    /*
    allenDocument = docDBClient.replaceDocument(
      allenDocument.getSelfLink, new Document(""), requestOptions).getResource

    docDBClient.deleteDocument(allenDocument.getSelfLink, requestOptions)
    */
  }
}
