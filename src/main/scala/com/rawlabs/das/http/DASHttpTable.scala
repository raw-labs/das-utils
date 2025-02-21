/*
 * Copyright 2024 RAW Labs S.A.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */

package com.rawlabs.das.http

import java.net.URI
import java.net.http.{HttpRequest, HttpResponse}

import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.das.sdk.{DASExecuteResult, DASSdkException}
import com.rawlabs.protocol.das.v1.query.{Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.{
  Column => ProtoColumn,
  ColumnDefinition,
  Row => ProtoRow,
  TableDefinition,
  TableId
}
import com.rawlabs.protocol.das.v1.types.{StringType, Type, Value, ValueString}
import com.typesafe.scalalogging.StrictLogging

class DASHttpTable(connector: DASHttpConnector, val httpConfig: HttpTableConfig) extends DASTable with StrictLogging {

  /** Define columns in the schema. All are strings in this simple example. */
  private val colNames = Seq("url", "method", "response_status_code", "request_headers", "response_body")

  override def getTablePathKeys = Seq.empty
  override def getTableSortOrders(sortKeys: Seq[SortKey]) = sortKeys

  val tableDefinition: TableDefinition = {
    val builder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(httpConfig.name))
      .setDescription(s"HTTP endpoint for ${httpConfig.url}")

    colNames.foreach { colName =>
      val colDef = ColumnDefinition
        .newBuilder()
        .setName(colName)
        .setType(
          Type
            .newBuilder()
            .setString(StringType.newBuilder().setNullable(true)))
      builder.addColumns(colDef)
    }

    builder.build()
  }

  /** For now, trivial estimate: just one row. */
  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    DASTable.TableEstimate(expectedNumberOfRows = 1, avgRowWidthBytes = 1000)
  }

  override def explain(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): Seq[String] = {
    Seq(
      s"Fetch from URL: ${httpConfig.url}",
      s"Method: ${httpConfig.method}",
      s"Headers: ${httpConfig.headers.mkString(", ")}")
  }

  /**
   * Execute the HTTP request, produce rows. In this simple example, each table fetches from a **single** endpoint. We
   * produce exactly **one row** per table. If you wanted multiple requests or pagination, you’d do that here.
   */
  override def execute(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): DASExecuteResult = {

    // 1) Build request
    val builder = HttpRequest
      .newBuilder()
      .uri(URI.create(httpConfig.url))
      .method(httpConfig.method, HttpRequest.BodyPublishers.noBody())

    // Add user headers
    httpConfig.headers.foreach { case (k, v) => builder.header(k, v) }

    // 2) Send request
    val httpClient = connector.getHttpClient
    val httpRequest = builder.build()

    logger.info(s"Making HTTP request: ${httpConfig.method} ${httpConfig.url}")
    val response =
      httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString())

    // 3) Gather data into a single row
    val rowValues: Map[String, String] = Map(
      "url" -> httpConfig.url,
      "method" -> httpConfig.method,
      "response_status_code" -> response.statusCode().toString,
      "request_headers" -> httpConfig.headers.toString(),
      "response_body" -> response.body())

    // We'll produce exactly 1 row
    val row = ProtoRow.newBuilder()
    val wantedColumns = if (columns.isEmpty) colNames else columns

    // For each of the known columns, if requested, build the proto.
    colNames.foreach { col =>
      if (wantedColumns.contains(col)) {
        val valueStr = Value
          .newBuilder()
          .setString(ValueString.newBuilder().setV(rowValues.getOrElse(col, "")))
        val protoCol = ProtoColumn
          .newBuilder()
          .setName(col)
          .setData(valueStr)
        row.addColumns(protoCol)
      }
    }

    // Create an iterator with a single row
    val rowIter = Iterator(row.build())

    new DASExecuteResult {
      override def hasNext: Boolean = rowIter.hasNext
      override def next(): ProtoRow = rowIter.next()
      override def close(): Unit = { /* no-op */ }
    }
  }

  // We do not support writes
  override def insert(row: ProtoRow): ProtoRow =
    throw new DASSdkException("HTTP DAS is read-only: insert not supported.")

  override def update(rowId: Value, newRow: ProtoRow): ProtoRow =
    throw new DASSdkException("HTTP DAS is read-only: update not supported.")

  override def delete(rowId: Value): Unit =
    throw new DASSdkException("HTTP DAS is read-only: delete not supported.")

}
