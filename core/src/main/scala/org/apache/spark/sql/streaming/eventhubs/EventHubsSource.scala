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

package org.apache.spark.sql.streaming.eventhubs

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.eventhubscommon.{EventHubNameAndPartition, EventHubsConnector, RateControlUtils}
import org.apache.spark.eventhubscommon.client.{EventHubClient, EventHubsClientWrapper, RestfulEventHubClient}
import org.apache.spark.eventhubscommon.progress.ProgressTrackerBase
import org.apache.spark.eventhubscommon.rdd.{EventHubsRDD, OffsetRange, OffsetStoreParams}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset, Source}
import org.apache.spark.sql.types._

/**
 * each source is mapped to an eventhubs instance
 */
private[spark] class EventHubsSource(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    eventhubReceiverCreator: (Map[String, String], Int, Long, Int) => EventHubsClientWrapper =
      EventHubsClientWrapper.getEventHubReceiver,
    eventhubClientCreator: (String, Map[String, Map[String, String]]) => EventHubClient =
      RestfulEventHubClient.getInstance) extends Source with EventHubsConnector with Logging {

  case class EventHubsOffset(batchId: Long, offsets: Map[EventHubNameAndPartition, (Long, Long)])

  private val eventhubsNamespace: String = parameters("eventhubs.namespace")
  private val eventhubsName: String = parameters("eventhubs.name")

  require(eventhubsNamespace != null, "eventhubs.namespace is not defined")
  require(eventhubsName != null, "eventhubs.name is not defined")

  private var _eventHubClient: EventHubClient = _

  private[eventhubs] def eventHubClient = {
    if (_eventHubClient == null) {
      _eventHubClient = eventhubClientCreator(eventhubsNamespace, Map(eventhubsName -> parameters))
    }
    _eventHubClient
  }

  private val ehNameAndPartitions = {
    val partitionCount = parameters("eventhubs.partition.count").toInt
    (for (partitionId <- 0 until partitionCount)
      yield EventHubNameAndPartition(eventhubsName, partitionId)).toList
  }

  ProgressTrackerBase.registeredConnectors += this

  // initialize ProgressTracker
  private val progressTracker = ProgressTrackerBase.initInstance(
    parameters("eventhubs.progressTrackingDir"), sqlContext.sparkContext.appName,
    sqlContext.sparkContext.hadoopConfiguration, "structuredstreaming")

  private[eventhubs] def setEventHubClient(eventHubClient: EventHubClient): EventHubsSource = {
    _eventHubClient = eventHubClient
    this
  }

  private var firstBatch = true
  private var committedOffsetsAndSeqNums: EventHubsOffset =
    EventHubsOffset(-1L, ehNameAndPartitions.map((_, (-1L, -1L))).toMap)
  private var fetchedHighestOffsetsAndSeqNums: EventHubsOffset = _

  override def schema: StructType = {
    val userDefinedKeys = parameters.get("eventhubs.sql.userDefinedKeys") match {
      case Some(keys) =>
        keys.split(",").toSeq
      case None =>
        Seq()
    }
    EventHubsSourceProvider.sourceSchema(userDefinedKeys)
  }

  private[spark] def composeHighestOffset(retryIfFail: Boolean) = {
    RateControlUtils.fetchLatestOffset(eventHubClient, retryIfFail = retryIfFail) match {
      case Some(highestOffsets) =>
        fetchedHighestOffsetsAndSeqNums = EventHubsOffset(committedOffsetsAndSeqNums.batchId,
          highestOffsets)
        Some(fetchedHighestOffsetsAndSeqNums.offsets)
      case _ =>
        logWarning(s"failed to fetch highest offset")
        if (retryIfFail) {
          None
        } else {
          Some(fetchedHighestOffsetsAndSeqNums.offsets)
        }
    }
  }

  /**
   * when we have reached the end of the message queue in the remote end or we haven't get any
   * idea about the highest offset, we shall fail the app when rest endpoint is not responsive, and
   * to prevent us from dying too much, we shall retry with 2-power interval in this case
   */
  private def failAppIfRestEndpointFail = fetchedHighestOffsetsAndSeqNums == null ||
    committedOffsetsAndSeqNums.offsets.equals(fetchedHighestOffsetsAndSeqNums.offsets)

  /**
   * @return return the target offset in the next batch
   */
  override def getOffset: Option[Offset] = {
    val highestOffsetsOpt = composeHighestOffset(failAppIfRestEndpointFail)
    require(highestOffsetsOpt.isDefined, "cannot get highest offset from rest endpoint of" +
      " eventhubs")
    if (!firstBatch) {
      updateCommittedOffsetsAndSeqNumsAndCommit(committedOffsetsAndSeqNums.batchId)
    } else {
      // use the initial
      firstBatch = false
    }
    val targetOffsets = RateControlUtils.clamp(committedOffsetsAndSeqNums.offsets,
      highestOffsetsOpt.get, parameters)
    Some(EventHubsBatchRecord(committedOffsetsAndSeqNums.batchId + 1,
      targetOffsets.map{case (ehNameAndPartition, seqNum) =>
        (ehNameAndPartition, math.min(seqNum,
          fetchedHighestOffsetsAndSeqNums.offsets(ehNameAndPartition)._2))}))
  }

  private def updateCommittedOffsetsAndSeqNumsAndCommit(committedBatchId: Long): Unit = {
    val lastFinishedBatchId = committedBatchId + 1
    committedOffsetsAndSeqNums = fetchEndingOffsetOfLastBatch(lastFinishedBatchId)
    progressTracker.commit(Map(uid -> committedOffsetsAndSeqNums.offsets), lastFinishedBatchId)
  }

  private def fetchEndingOffsetOfLastBatch(committedBatchId: Long) = {
    val startOffsetOfUndergoingBatch = progressTracker.collectProgressRecordsForBatch(
      committedBatchId)
    if (startOffsetOfUndergoingBatch.isEmpty) {
      // first batch, take the initial value of the offset, -1
      EventHubsOffset(committedBatchId, committedOffsetsAndSeqNums.offsets)
    } else {
      EventHubsOffset(committedBatchId,
        startOffsetOfUndergoingBatch.filter { case (connectorUID, _) =>
          connectorUID == uid
        }.values.head.filter(_._1.eventHubName == parameters("eventhubs.name")))
    }
  }

  private def buildEventHubsRDD(endOffset: EventHubsBatchRecord): EventHubsRDD = {
    val offsetRanges = fetchedHighestOffsetsAndSeqNums.offsets.map {
      case (eventHubNameAndPartition, (_, endSeqNum)) =>
        OffsetRange(eventHubNameAndPartition,
          fromOffset = committedOffsetsAndSeqNums.offsets(eventHubNameAndPartition)._1,
          fromSeq = committedOffsetsAndSeqNums.offsets(eventHubNameAndPartition)._2,
          untilSeq = endOffset.targetSeqNums(eventHubNameAndPartition))
    }.toList
    new EventHubsRDD(
      sqlContext.sparkContext,
      Map(parameters("eventhubs.name") -> parameters),
      offsetRanges,
      committedOffsetsAndSeqNums.batchId + 1,
      OffsetStoreParams(parameters("eventhubs.progressTrackingDir"),
        sqlContext.sparkContext.appName, streamId, uid),
      eventhubReceiverCreator
    )
  }

  private def convertEventHubsRDDToDataFrame(eventHubsRDD: EventHubsRDD): DataFrame = {
    import scala.collection.JavaConverters._
    val internalRowRDD = eventHubsRDD.map(eventData =>
      InternalRow.fromSeq(Seq(eventData.getBody, eventData.getSystemProperties.getOffset.toLong,
        eventData.getSystemProperties.getSequenceNumber,
        eventData.getSystemProperties.getEnqueuedTime.getEpochSecond,
        eventData.getSystemProperties.getPublisher,
        eventData.getSystemProperties.getPartitionKey
      ) ++ eventData.getProperties.asScala.values)
    )
    sqlContext.internalCreateDataFrame(internalRowRDD, schema)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    if (committedOffsetsAndSeqNums.batchId == -1) {
      // in this case, we are just recovering from a failure; the committedOffsets and
      // availableOffsets are fetched from in populateStartOffset() of StreamExecution
      // convert (committedOffsetsAndSeqNums is in initial state)
      updateCommittedOffsetsAndSeqNumsAndCommit(start.map {
        case so: SerializedOffset =>
          val batchRecord = JsonUtils.partitionAndSeqNum(so.json)
          batchRecord.asInstanceOf[EventHubsBatchRecord].batchId
        case batchRecord: EventHubsBatchRecord =>
          batchRecord.batchId
      }.getOrElse(0L))
      val highestOffsets = composeHighestOffset(failAppIfRestEndpointFail)
      require(highestOffsets.isDefined, "cannot get highest offsets when recovering from a failure")
      fetchedHighestOffsetsAndSeqNums = EventHubsOffset(committedOffsetsAndSeqNums.batchId,
        highestOffsets.get)
      firstBatch = false
    }
    val eventhubsRDD = buildEventHubsRDD({
      end match {
        case so: SerializedOffset =>
          JsonUtils.partitionAndSeqNum(so.json)
        case batchRecord: EventHubsBatchRecord =>
          batchRecord
      }
    })
    convertEventHubsRDDToDataFrame(eventhubsRDD)
  }

  override def stop(): Unit = {}

  // uniquely identify the entities in eventhubs side, it can be the namespace or the name of a
  override def uid: String = s"${eventhubsNamespace}_$eventhubsName"

  // the list of eventhubs partitions connecting with this connector
  override def connectedInstances: List[EventHubNameAndPartition] = ehNameAndPartitions

  // the id of the stream which is mapped from eventhubs instance
  override val streamId: Int = EventHubsSource.streamIdGenerator.getAndIncrement()
}

private object EventHubsSource {
  val streamIdGenerator = new AtomicInteger(0)
}
