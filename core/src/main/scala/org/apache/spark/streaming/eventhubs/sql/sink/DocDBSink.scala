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

import com.microsoft.azure.documentdb.{ConnectionPolicy, ConsistencyLevel, Document, DocumentClient}

import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class DocDBSink(databaseId: String, collectionId: String) extends Sink {

  val client = new DocumentClient(
    "<your endpoint URI>",
    "<your key>"
    , new ConnectionPolicy(),
    ConsistencyLevel.Session)

  val doc = new Document()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    // step 1: translate DF to json
    // convert every row to json

    // write to DocumentDB atomically
    // data.write.
  }

}
