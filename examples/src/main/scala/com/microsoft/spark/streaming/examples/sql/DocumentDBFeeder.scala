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

import com.microsoft.azure.documentdb._

object DocumentDBFeeder {


  def main(args: Array[String]): Unit = {

    val END_POINT = args(0)
    val MASTER_KEY = args(1)
    val DATABASE_ID = args(2)
    val COLLECTION_ID = args(3)

    val docDBClient = new DocumentClient(END_POINT, MASTER_KEY, ConnectionPolicy.GetDefault(),
      ConsistencyLevel.BoundedStaleness)

    val myDB = new Database()
    myDB.setId(DATABASE_ID)

    // Create a new database.
    // TODO: try if it throws exception when DB exists
    val newDB = docDBClient.createDatabase(myDB, null).getResource

    var myCollection = new DocumentCollection()
    myCollection.setId(COLLECTION_ID)

    val requestOptions = new RequestOptions()
    requestOptions.setOfferThroughput(1000)

    myCollection = docDBClient.createCollection(
      "dbs/" + DATABASE_ID, myCollection, requestOptions).getResource

    System.out.println("Created a new collection:")
    System.out.println(myCollection.toString())


    var allenDocument = new Document("")
    allenDocument = docDBClient.createDocument(
      "dbs/" + DATABASE_ID + "/colls/" + COLLECTION_ID, allenDocument, requestOptions, false)
      .getResource

    /*
    allenDocument = docDBClient.replaceDocument(
      allenDocument.getSelfLink, new Document(""), requestOptions).getResource

    docDBClient.deleteDocument(allenDocument.getSelfLink, requestOptions)
    */
  }
}
