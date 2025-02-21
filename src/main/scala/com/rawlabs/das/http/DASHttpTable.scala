/*
 * Copyright 2025 RAW Labs S.A.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */

package com.rawlabs.das.htttp

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.das.sdk.{DASExecuteResult, DASSdkException}
import com.rawlabs.protocol.das.v1.query.{Operator, PathKey, Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.{
  Column => ProtoColumn,
  ColumnDefinition,
  Row => ProtoRow,
  TableDefinition,
  TableId
}
import com.rawlabs.protocol.das.v1.types.{StringType, Type, Value, ValueString}

/**
 * A single table that reads its parameters from the WHERE clause. The table name is `net_http_request`.
 */
class DASHttpTable extends DASTable {

  // Columns
  private val colNames =
    Seq("url", "method", "request_headers", "request_body", "response_status_code", "response_body")

  private val tableName = "net_http_request"

  /** Define our table schema. */
  val tableDefinition: TableDefinition = {
    val builder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableName))
      .setDescription("A single table that performs HTTP requests, with parameters from the WHERE clause.")

    colNames.foreach { col =>
      val colDef = ColumnDefinition
        .newBuilder()
        .setName(col)
        .setType(Type.newBuilder().setString(StringType.newBuilder().setNullable(true)))
      builder.addColumns(colDef)
    }

    builder.build()
  }

  // No path/sort pushdown
  override def getTablePathKeys: Seq[PathKey] = Seq.empty
  override def getTableSortOrders(sortKeys: Seq[SortKey]): Seq[SortKey] = sortKeys

  /**
   * We produce exactly 1 row per query, so row estimate is 1.
   */
  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    DASTable.TableEstimate(expectedNumberOfRows = 1, avgRowWidthBytes = 1000)
  }

  /** For debugging, show how we plan to fetch. */
  override def explain(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): Seq[String] =
    Seq(s"Single HTTP request, parameters from WHERE clause => columns: ${columns.mkString(", ")}")

  /**
   * The core: parse the WHERE clause, build an HTTP request, perform it, produce a single row.
   */
  override def execute(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): DASExecuteResult = {

    val paramMap = parseQuals(quals)

    // Extract parameters with defaults
    val url = paramMap.getOrElse("url", "http://httpbin.org/get")
    val method = paramMap.getOrElse("method", "GET").toUpperCase
    val rawHeader = paramMap.getOrElse("request_headers", "")
    val bodyStr = paramMap.getOrElse("request_body", "")

    // Build a simple headers map from rawHeader string "Key:Val,Key2:Val2"
    val headers: Map[String, String] =
      rawHeader
        .split(",")
        .map(_.trim)
        .filter(_.nonEmpty)
        .flatMap { pair =>
          val parts = pair.split(":", 2).map(_.trim)
          if (parts.length == 2) Some(parts(0) -> parts(1)) else None
        }
        .toMap

    // Make the HTTP request
    val client = HttpClient.newHttpClient()
    val builder = HttpRequest.newBuilder().uri(URI.create(url))

    method match {
      // If POST or PUT, use the body
      case "POST" => builder.POST(HttpRequest.BodyPublishers.ofString(bodyStr))
      case "PUT"  => builder.PUT(HttpRequest.BodyPublishers.ofString(bodyStr))
      // Otherwise, default to GET
      case _ => builder.GET()
    }

    // Add headers
    headers.foreach { case (k, v) => builder.header(k, v) }

    val request = builder.build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    val statusCode = response.statusCode().toString
    val responseBody = response.body()

    // Build the row. We produce only one row. (Or you could produce none if you prefer.)
    val rowValues = Map(
      "url" -> url,
      "method" -> method,
      "request_headers" -> rawHeader,
      "request_body" -> bodyStr,
      "response_status_code" -> statusCode,
      "response_body" -> responseBody)

    val wantedCols = if (columns.isEmpty) colNames else columns

    val rowBuilder = ProtoRow.newBuilder()
    colNames.foreach { col =>
      if (wantedCols.contains(col)) {
        val value = Value
          .newBuilder()
          .setString(ValueString.newBuilder().setV(rowValues.getOrElse(col, "")))
        val protoCol = ProtoColumn
          .newBuilder()
          .setName(col)
          .setData(value)
        rowBuilder.addColumns(protoCol)
      }
    }

    val singleRow = rowBuilder.build()
    val rowIter = Iterator(singleRow)

    new DASExecuteResult {
      override def hasNext: Boolean = rowIter.hasNext
      override def next(): ProtoRow = rowIter.next()
      override def close(): Unit = {}
    }
  }

  // Read-only: no inserts/updates/deletes
  override def insert(row: ProtoRow): ProtoRow =
    throw new DASSdkException("HTTP single-table DAS is read-only.")
  override def update(rowId: Value, newRow: ProtoRow): ProtoRow =
    throw new DASSdkException("HTTP single-table DAS is read-only.")
  override def delete(rowId: Value): Unit =
    throw new DASSdkException("HTTP single-table DAS is read-only.")

  // Helper: parse the simple Quals as a Map[String, String]
  // We only handle "col = 'value'" for columns: url, method, request_headers, request_body
  private def parseQuals(quals: Seq[Qual]): Map[String, String] = {
    // Columns we care about
    val validCols = Set("url", "method", "request_headers", "request_body")

    val builder = Map.newBuilder[String, String]

    for (q <- quals) {
      val colName = q.getName // The "name" field in Qual
      // Check that colName is in our known set
      if (validCols.contains(colName)) {
        // Check if this Qual is a SimpleQual
        q.getQualCase match {
          case Qual.QualCase.SIMPLE_QUAL =>
            val sq = q.getSimpleQual
            // We only handle 'operator == EQUALS' here
            if (sq.getOperator == Operator.EQUALS) {
              // Now check if the value is a string (ValueString)
              val v = sq.getValue
              if (v.hasString) {
                builder += (colName -> v.getString.getV)
              }
            }

          // If it's IS_ANY_QUAL or IS_ALL_QUAL, or any other operator, we ignore it here
          case _ => ()
        }
      }
    }
    builder.result()
  }
}
