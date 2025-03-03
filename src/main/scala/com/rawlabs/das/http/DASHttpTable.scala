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

package com.rawlabs.das.http
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl._

import scala.jdk.CollectionConverters._

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
import com.rawlabs.protocol.das.v1.types._

/**
 * A single-table HTTP DAS that retrieves data from an HTTP endpoint
 */
class DASHttpTable extends DASTable {

  // Define columns with their protobuf type
  private val columns: Seq[(String, Type)] = Seq(
    "url" -> mkStringType(),
    "method" -> mkStringType(),
    // request_headers => jsonb record
    "request_headers" -> mkRecordType(),
    // url_args => jsonb record
    "url_args" -> mkRecordType(),
    "request_body" -> mkStringType(),
    "follow_redirect" -> mkBoolType(),
    "connect_timeout_millis" -> mkIntType(),
    "request_timeout_millis" -> mkIntType(),
    "ssl_trust_all" -> mkBoolType(),
    // Output columns
    "response_status_code" -> mkIntType(),
    "response_body" -> mkStringType(),
    "response_headers" -> mkRecordType())

  private val tableName = "http_request"

  /** Build a TableDefinition from the columns above. */
  val tableDefinition: TableDefinition = {
    val builder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableName))
      .setDescription("Single-table HTTP plugin.")

    columns.foreach { case (colName, colType) =>
      builder.addColumns(
        ColumnDefinition
          .newBuilder()
          .setName(colName)
          .setType(colType))
    }
    builder.build()
  }

  override def getTablePathKeys: Seq[PathKey] = Seq.empty
  override def getTableSortOrders(sortKeys: Seq[SortKey]): Seq[SortKey] = Seq.empty

  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    // Single HTTP request => single row
    DASTable.TableEstimate(expectedNumberOfRows = 1, avgRowWidthBytes = 1000)
  }

  override def explain(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): Seq[String] =
    Seq("HTTP single table.")

  /**
   * The core: parse WHERE clause => build & send HTTP => produce 1 row => yield columns
   */
  override def execute(
      quals: Seq[Qual],
      columnsRequested: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): DASExecuteResult = {

    // 1) Parse the typed parameters from Quals
    val params = parseQuals(quals)

    // 2) Validate required fields
    val url = params.url.getOrElse {
      throw new DASSdkException("Missing 'url' in WHERE clause")
    }

    val method = params.method.getOrElse("GET").toUpperCase
    val requestHeaders = params.requestHeaders.getOrElse(Map.empty)
    val urlArgs = params.urlArgs.getOrElse(Map.empty)
    val body = params.requestBody.getOrElse("")
    val followRedirect = params.followRedirect.getOrElse(false)
    val connectTimeoutMillis = params.connectTimeoutMillis.getOrElse(5000)
    val requestTimeoutMillis = params.requestTimeoutMillis.getOrElse(30000)
    val sslTrustAll = params.sslTrustAll.getOrElse(false)

    // 3) Build HttpClient with connect timeout and optional SSL trust-all
    val client = buildHttpClient(followRedirect, connectTimeoutMillis, sslTrustAll)

    try {
      // 4) Construct the final URL with urlArgs if you like (like "?foo=bar&test=123").
      //    Or you can handle them differently. For simplicity, let's assume we just do "baseUrl?arg1&arg2"
      val finalUrl = buildUrlWithArgs(url, urlArgs)

      // 5) Create request
      val requestBuilder =
        try {
          HttpRequest.newBuilder().uri(URI.create(finalUrl))
        } catch {
          case ex: IllegalArgumentException =>
            throw new DASSdkException(s"Invalid url: ${ex.getMessage}", ex)
        }

      requestBuilder.timeout(java.time.Duration.ofMillis(requestTimeoutMillis))

      method.toUpperCase match {
        case "GET"     => requestBuilder.GET()
        case "DELETE"  => requestBuilder.DELETE()
        case "POST"    => requestBuilder.POST(HttpRequest.BodyPublishers.ofString(body))
        case "PUT"     => requestBuilder.PUT(HttpRequest.BodyPublishers.ofString(body))
        case "PATCH"   => requestBuilder.method("PATCH", HttpRequest.BodyPublishers.ofString(body))
        case "HEAD"    => requestBuilder.HEAD()
        case "OPTIONS" => requestBuilder.method("OPTIONS", HttpRequest.BodyPublishers.ofString(body))
        case unknown   => throw new DASSdkException(s"Unsupported HTTP method: $unknown")
      }

      // 6) Add request headers
      requestHeaders.foreach { case (k, v) => requestBuilder.header(k, v) }

      // 7) Send
      val response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())
      val statusCode = response.statusCode()
      val respBody = response.body()

      // 8) Build a single row with all columns
      val rowBuilder = ProtoRow.newBuilder()

      // If the user didn't request any columns, we default to returning all. Or
      // if columnsRequested is empty, let's say we return them all anyway.
      val wantedCols = if (columnsRequested.isEmpty) columns.map(_._1) else columnsRequested

      // Convert everything into a Map of col -> Value
      val colValues: Map[String, Value] = Map(
        "url" -> mkStringValue(url),
        "method" -> mkStringValue(method),
        "request_headers" -> mkRecordValue(requestHeaders),
        "url_args" -> mkRecordValue(urlArgs),
        "request_body" -> mkStringValue(body),
        "follow_redirect" -> Value.newBuilder().setBool(ValueBool.newBuilder().setV(followRedirect)).build(),
        "response_status_code" -> Value.newBuilder().setInt(ValueInt.newBuilder().setV(statusCode)).build(),
        "response_body" -> mkStringValue(respBody),
        "response_headers" -> mkRecordValue(
          // flatten header values into a single string
          response.headers().map().asScala.map { case (key, values) => key -> values.asScala.mkString(",") }.toMap))

      // Add them if in wantedCols
      columns.foreach { case (colName, _) =>
        if (wantedCols.contains(colName)) {
          val valObj = colValues.getOrElse(colName, mkStringValue(""))
          rowBuilder.addColumns(
            ProtoColumn
              .newBuilder()
              .setName(colName)
              .setData(valObj))
        }
      }

      val rowIter = Iterator(rowBuilder.build())

      new DASExecuteResult {
        override def hasNext: Boolean = rowIter.hasNext

        override def next(): ProtoRow = rowIter.next()

        override def close(): Unit = {}
      }

    } catch {
      case ex: java.net.http.HttpTimeoutException =>
        throw new DASSdkException(s"Request timed out: ${ex.getMessage}", ex)
      case ex: java.net.UnknownHostException =>
        throw new DASSdkException(s"Unknown host: ${ex.getMessage}", ex)
      case ex: java.net.ConnectException =>
        throw new DASSdkException(s"Connection error: ${ex.getMessage}", ex)
      case ex: javax.net.ssl.SSLException =>
        throw new DASSdkException(s"SSL error: ${ex.getMessage}", ex)
      case ex: java.io.IOException =>
        throw new DASSdkException(s"Network I/O error: ${ex.getMessage}", ex)
      case ex: IllegalArgumentException =>
        // e.g., malformed URL or invalid arguments
        throw new DASSdkException(s"Invalid request parameter: ${ex.getMessage}", ex)
    } finally {
      client.close()
    }
  }

  // read-only
  override def insert(row: ProtoRow): ProtoRow =
    throw new DASSdkException("HTTP single-table is read-only.")
  override def update(rowId: Value, newRow: ProtoRow): ProtoRow =
    throw new DASSdkException("HTTP single-table is read-only.")
  override def delete(rowId: Value): Unit =
    throw new DASSdkException("HTTP single-table is read-only.")

  /**
   * Helper to build an HttpClient with connect-timeout, SSL trust-all, and redirect handling.
   */
  private def buildHttpClient(followRedirect: Boolean, connectTimeoutMillis: Int, sslTrustAll: Boolean): HttpClient = {
    val builder = HttpClient.newBuilder()

    // Follow redirects if set
    if (followRedirect) builder.followRedirects(HttpClient.Redirect.ALWAYS)
    else builder.followRedirects(HttpClient.Redirect.NEVER)

    // Connect timeout
    builder.connectTimeout(java.time.Duration.ofMillis(connectTimeoutMillis))

    // SSL trust all
    if (sslTrustAll) {
      val trustAllCerts = Array[TrustManager](new X509TrustManager {
        override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
        override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
        override def getAcceptedIssuers: Array[X509Certificate] = Array.empty
      })

      val sslContext = SSLContext.getInstance("TLS")
      sslContext.init(null, trustAllCerts, new SecureRandom())
      builder.sslContext(sslContext)

      // also disable hostname verification
      val noVerification = new SSLParameters()
      noVerification.setEndpointIdentificationAlgorithm(null)
      builder.sslParameters(noVerification)
    }

    builder.build()
  }
  // --------------------------------------------------------------------------------
  // Helper: parse typed columns from Quals
  // --------------------------------------------------------------------------------

  /**
   * We define a small container for the typed columns that can appear in WHERE.
   */
  private case class ParsedParams(
      url: Option[String] = None,
      method: Option[String] = None,
      requestHeaders: Option[Map[String, String]] = None,
      urlArgs: Option[Map[String, String]] = None,
      requestBody: Option[String] = None,
      followRedirect: Option[Boolean] = None,
      connectTimeoutMillis: Option[Int] = None,
      requestTimeoutMillis: Option[Int] = None,
      sslTrustAll: Option[Boolean] = None)

  /**
   * parseQuals converts Quals to typed fields. We only handle operator == EQUALS, and we check the Value's type
   * carefully, throwing if it doesn't match.
   */
  private def parseQuals(quals: Seq[Qual]): ParsedParams = {
    var result = ParsedParams()

    for (q <- quals) {
      val colName = q.getName

      // We only handle simpleQual
      if (!q.hasSimpleQual) {
        throw new DASSdkException(s"Column '$colName' must be a simple equality.")
      }

      val sq = q.getSimpleQual

      // We only handle operator = EQUALS
      if (sq.getOperator != Operator.EQUALS) {
        throw new DASSdkException(s"Only EQUALS operator is supported. Found ${sq.getOperator} for column '$colName'.")
      }

      val v = sq.getValue

      colName match {
        case "url" =>
          if (!v.hasString) {
            throw new DASSdkException("Column 'url' must be a string value.")
          }
          result = result.copy(url = Some(v.getString.getV))

        case "method" =>
          if (!v.hasString) {
            throw new DASSdkException("Column 'method' must be a string value.")
          }
          result = result.copy(method = Some(v.getString.getV))

        case "request_body" =>
          if (!v.hasString) {
            throw new DASSdkException("Column 'request_body' must be a string value.")
          }
          result = result.copy(requestBody = Some(v.getString.getV))

        case "follow_redirect" =>
          if (!v.hasBool) {
            throw new DASSdkException("Column 'follow_redirect' must be a boolean value.")
          }
          result = result.copy(followRedirect = Some(v.getBool.getV))

        case "connect_timeout_millis" =>
          if (!v.hasInt) {
            throw new DASSdkException("Column 'connect_timeout_millis' must be an integer value.")
          }
          result = result.copy(connectTimeoutMillis = Some(v.getInt.getV))
        case "request_timeout_millis" =>
          if (!v.hasInt) {
            throw new DASSdkException("Column 'request_timeout_millis' must be an integer value.")
          }
          result = result.copy(requestTimeoutMillis = Some(v.getInt.getV))
        case "ssl_trust_all" =>
          if (!v.hasBool) {
            throw new DASSdkException("Column 'ssl_trust_all' must be a boolean value.")
          }
          result = result.copy(sslTrustAll = Some(v.getBool.getV))
        case "request_headers" =>
          if (!v.hasRecord) {
            throw new DASSdkException("Column 'request_headers' must be a record.")
          }
          val rec = v.getRecord
          val items = rec.getAttsList.asScala.toList.map { it =>
            val key = it.getName
            val value = it.getValue
            if (!value.hasString) {
              throw new DASSdkException("Column 'request_headers' must be a record of string values.")
            }
            key -> value.getString.getV
          }
          result = result.copy(requestHeaders = Some(items.toMap))

        case "url_args" =>
          if (!v.hasRecord) {
            throw new DASSdkException("Column 'url_args' must be a record.")
          }
          val rec = v.getRecord
          val items = rec.getAttsList.asScala.toList.map { it =>
            val key = it.getName
            val value = it.getValue
            if (!value.hasString) {
              throw new DASSdkException("Column 'url_args' must be a record of string values.")
            }
            key -> value.getString.getV
          }
          result = result.copy(urlArgs = Some(items.toMap))

        // If you want to **throw** on unknown columns:
        case unknown =>
          throw new DASSdkException(s"Column '$unknown' is not recognized by this DAS.")
      }
    }
    result
  }
  // --------------------------------------------------------------------------------
  // Additional helper methods
  // --------------------------------------------------------------------------------

  /**
   * Build the final URL with appended query args if needed. E.g., if urlArgs has ["foo=bar","debug=true"], we do
   * "?foo=bar&debug=true".
   */
  private def buildUrlWithArgs(baseUrl: String, args: Map[String, String]): String = {
    if (args.isEmpty) baseUrl
    else {
      val sep = if (baseUrl.contains("?")) "&" else "?"
      val encoded = args.map { case (k, v) => k + "=" + java.net.URLEncoder.encode(v, "UTF-8") }.mkString("&")
      s"$baseUrl$sep$encoded"
    }
  }

  private def mkStringValue(s: String): Value = {
    Value.newBuilder().setString(ValueString.newBuilder().setV(s)).build()
  }

  private def mkRecordValue(values: Map[String, String]) = {
    val recordBuilder = ValueRecord.newBuilder()
    val atts = values.map { case (k, v) =>
      val value = Value.newBuilder().setString(ValueString.newBuilder().setV(v)).build()
      ValueRecordAttr.newBuilder().setName(k).setValue(value).build()

    }
    recordBuilder.addAllAtts(atts.asJava)
    Value.newBuilder().setRecord(recordBuilder).build()
  }

  private def mkStringType(): Type = {
    val builder = Type.newBuilder().setString(StringType.newBuilder().setNullable(false))
    builder.build()
  }

  private def mkBoolType(): Type = {
    val builder = Type.newBuilder().setBool(BoolType.newBuilder().setNullable(false))
    builder.build()
  }

  private def mkIntType(): Type = {
    val builder = Type.newBuilder().setInt(IntType.newBuilder().setNullable(false))
    builder.build()
  }

  private def mkRecordType(): Type = {
    val builder = Type.newBuilder().setRecord(RecordType.newBuilder().setNullable(false))
    builder.build()
  }
}
