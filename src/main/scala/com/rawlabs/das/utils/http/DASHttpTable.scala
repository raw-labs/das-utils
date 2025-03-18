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

package com.rawlabs.das.utils.http

import java.net.URI
import java.net.http.{HttpClient, HttpHeaders, HttpRequest, HttpResponse}
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl._

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.das.sdk.{DASExecuteResult, DASSdkInvalidArgumentException}
import com.rawlabs.protocol.das.v1.query.{Operator, PathKey, Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.{
  Column => ProtoColumn,
  ColumnDefinition,
  Row => ProtoRow,
  TableDefinition,
  TableId
}
import com.rawlabs.protocol.das.v1.types._
import com.typesafe.scalalogging.StrictLogging

/**
 * A single-table HTTP DAS that retrieves data from an HTTP endpoint
 */
class DASHttpTable extends DASTable with StrictLogging {

  // Define columns with their protobuf type
  private val columns: Seq[(String, Type)] = Seq(
    "url" -> mkStringType(),
    "method" -> mkStringType(),
    "request_headers" -> mkRecordType(), // jsonb record
    "url_args" -> mkRecordType(), // jsonb record
    "request_body" -> mkStringType(),
    "follow_redirects" -> mkBoolType(),
    "connect_timeout_millis" -> mkIntType(),
    "request_timeout_millis" -> mkIntType(),
    "ssl_trust_all" -> mkBoolType(),
    // Output columns
    "response_status_code" -> mkIntType(),
    "response_body" -> mkStringType(),
    "response_headers" -> mkRecordType()
  ) // jsonb record

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
      throw new DASSdkInvalidArgumentException("Missing 'url' in WHERE clause")
    }

    val method = params.method.getOrElse(ValueString.newBuilder().setV("GET").build())
    val requestHeaders = params.requestHeaders.getOrElse(ValueRecord.newBuilder().build())
    val urlArgs = params.urlArgs.getOrElse(ValueRecord.newBuilder().build())
    val body = params.requestBody.getOrElse(ValueString.newBuilder().setV("").build())
    val followRedirects = params.followRedirects.getOrElse(ValueBool.newBuilder().setV(false).build())
    val connectTimeoutMillis = params.connectTimeoutMillis.getOrElse(ValueInt.newBuilder().setV(5000).build())
    val requestTimeoutMillis = params.requestTimeoutMillis.getOrElse(ValueInt.newBuilder().setV(30000).build())
    val sslTrustAll = params.sslTrustAll.getOrElse(ValueBool.newBuilder().setV(false).build())

    // 3) Build HttpClient with connect timeout and optional SSL trust-all
    val client = buildHttpClient(followRedirects.getV, connectTimeoutMillis.getV, sslTrustAll.getV)

    try {
      // 4) Construct the final URL with urlArgs if you like (like "?foo=bar&test=123").
      //    Or you can handle them differently. For simplicity, let's assume we just do "baseUrl?arg1&arg2"
      val finalUrl = buildUrlWithArgs(url.getV, urlArgs)

      // 5) Create request
      val requestBuilder =
        try {
          HttpRequest.newBuilder().uri(URI.create(finalUrl))
        } catch {
          case ex: IllegalArgumentException =>
            throw new DASSdkInvalidArgumentException(s"Invalid url: ${ex.getMessage}", ex)
        }

      requestBuilder.timeout(java.time.Duration.ofMillis(requestTimeoutMillis.getV))

      method.getV.toUpperCase match {
        case "GET"     => requestBuilder.GET()
        case "DELETE"  => requestBuilder.DELETE()
        case "POST"    => requestBuilder.POST(HttpRequest.BodyPublishers.ofString(body.getV))
        case "PUT"     => requestBuilder.PUT(HttpRequest.BodyPublishers.ofString(body.getV))
        case "PATCH"   => requestBuilder.method("PATCH", HttpRequest.BodyPublishers.ofString(body.getV))
        case "HEAD"    => requestBuilder.HEAD()
        case "OPTIONS" => requestBuilder.method("OPTIONS", HttpRequest.BodyPublishers.ofString(body.getV))
        case unknown   => throw new DASSdkInvalidArgumentException(s"Unsupported HTTP method: $unknown")
      }

      // 6) Add request headers
      addRequestHeaders(requestBuilder, requestHeaders)

      // 7) Send
      val response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())
      // 8) Build a single row with all columns
      val rowBuilder = ProtoRow.newBuilder()

      // If the user didn't request any columns, we default to returning all. Or
      // if columnsRequested is empty, let's say we return them all anyway.
      val wantedCols = if (columnsRequested.isEmpty) columns.map(_._1) else columnsRequested

      // Convert everything into a Map of col -> Value
      val colValues: Map[String, Value] = Map(
        "url" -> Value.newBuilder().setString(url).build(),
        "method" -> Value.newBuilder().setString(method).build(),
        "request_headers" -> Value.newBuilder().setRecord(requestHeaders).build(),
        "url_args" -> Value.newBuilder().setRecord(urlArgs).build(),
        "request_body" -> Value.newBuilder().setString(body).build(),
        "follow_redirects" -> Value.newBuilder().setBool(followRedirects).build(),
        "response_status_code" -> mkInValue(response.statusCode()),
        "response_body" -> mkStringValue(response.body()),
        "response_headers" -> headersToRecordValue(response.headers()))

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
        throw new DASSdkInvalidArgumentException(s"Request timed out: ${ex.getMessage}", ex)
      case ex: java.net.UnknownHostException =>
        throw new DASSdkInvalidArgumentException(s"Unknown host: ${ex.getMessage}", ex)
      case ex: java.net.ConnectException =>
        throw new DASSdkInvalidArgumentException(s"Connection error: ${ex.getMessage}", ex)
      case ex: javax.net.ssl.SSLException =>
        throw new DASSdkInvalidArgumentException(s"SSL error: ${ex.getMessage}", ex)
      case ex: java.io.IOException =>
        throw new DASSdkInvalidArgumentException(s"Network I/O error: ${ex.getMessage}", ex)
      case ex: IllegalArgumentException =>
        // e.g., malformed URL or invalid arguments
        throw new DASSdkInvalidArgumentException(s"Invalid request parameter: ${ex.getMessage}", ex)
      case NonFatal(ex) =>
        logger.error("Unexpected error", ex)
        throw new DASSdkInvalidArgumentException(s"Unexpected error", ex)

    } finally {
      client.close()
    }
  }

  /**
   * Helper to build an HttpClient with connect-timeout, SSL trust-all, and redirect handling.
   */
  protected def buildHttpClient(
      followRedirect: Boolean,
      connectTimeoutMillis: Int,
      sslTrustAll: Boolean): HttpClient = {
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
      url: Option[ValueString] = None,
      method: Option[ValueString] = None,
      requestHeaders: Option[ValueRecord] = None,
      urlArgs: Option[ValueRecord] = None,
      requestBody: Option[ValueString] = None,
      followRedirects: Option[ValueBool] = None,
      connectTimeoutMillis: Option[ValueInt] = None,
      requestTimeoutMillis: Option[ValueInt] = None,
      sslTrustAll: Option[ValueBool] = None)

  /**
   * parseQuals converts Quals to typed fields. We only handle operator == EQUALS, and we check the Value's type
   * carefully, throwing if it doesn't match.
   */
  private def parseQuals(quals: Seq[Qual]): ParsedParams = {
    var result = ParsedParams()

    for (q <- quals) {
      val colName = q.getName

      colName match {
        case "url" =>
          // We only handle operator = EQUALS
          ensureEqualsOperator(q)
          val v = q.getSimpleQual.getValue
          if (!v.hasString) {
            throw new DASSdkInvalidArgumentException("Column 'url' must be a string value.")
          }
          result = result.copy(url = Some(v.getString))
        case "method" =>
          ensureEqualsOperator(q)
          val v = q.getSimpleQual.getValue
          if (!v.hasString) {
            throw new DASSdkInvalidArgumentException("Column 'method' must be a string value.")
          }
          result = result.copy(method = Some(v.getString))
        case "request_body" =>
          ensureEqualsOperator(q)
          val v = q.getSimpleQual.getValue
          if (!v.hasString) {
            throw new DASSdkInvalidArgumentException("Column 'request_body' must be a string value.")
          }
          result = result.copy(requestBody = Some(v.getString))
        case "follow_redirects" =>
          ensureEqualsOperator(q)
          val v = q.getSimpleQual.getValue
          if (!v.hasBool) {
            throw new DASSdkInvalidArgumentException("Column 'follow_redirect' must be a boolean value.")
          }
          result = result.copy(followRedirects = Some(v.getBool))
        case "connect_timeout_millis" =>
          ensureEqualsOperator(q)
          val v = q.getSimpleQual.getValue
          if (!v.hasInt) {
            throw new DASSdkInvalidArgumentException("Column 'connect_timeout_millis' must be an integer value.")
          }
          result = result.copy(connectTimeoutMillis = Some(v.getInt))
        case "request_timeout_millis" =>
          ensureEqualsOperator(q)
          val v = q.getSimpleQual.getValue
          if (!v.hasInt) {
            throw new DASSdkInvalidArgumentException("Column 'request_timeout_millis' must be an integer value.")
          }
          result = result.copy(requestTimeoutMillis = Some(v.getInt))
        case "ssl_trust_all" =>
          ensureEqualsOperator(q)
          val v = q.getSimpleQual.getValue
          if (!v.hasBool) {
            throw new DASSdkInvalidArgumentException("Column 'ssl_trust_all' must be a boolean value.")
          }
          result = result.copy(sslTrustAll = Some(v.getBool))
        case "request_headers" =>
          ensureEqualsOperator(q)
          val v = q.getSimpleQual.getValue
          if (!v.hasRecord) {
            throw new DASSdkInvalidArgumentException("Column 'request_headers' must be a record.")
          }
          result = result.copy(requestHeaders = Some(v.getRecord))
        case "url_args" =>
          ensureEqualsOperator(q)
          val v = q.getSimpleQual.getValue
          if (!v.hasRecord) {
            throw new DASSdkInvalidArgumentException("Column 'url_args' must be a record.")
          }
          result = result.copy(urlArgs = Some(v.getRecord))
        case _ =>

      }
    }
    result
  }
  // --------------------------------------------------------------------------------
  // Additional helper methods
  // --------------------------------------------------------------------------------

  private def ensureEqualsOperator(q: Qual): Unit = {
    if (!q.hasSimpleQual || q.getSimpleQual.getOperator != Operator.EQUALS) {
      throw new DASSdkInvalidArgumentException(s"Only EQUALS operator is supported for column ${q.getName}")
    }
  }

  /**
   * Build the final URL with appended query args if needed. E.g., if urlArgs has ["foo=bar","debug=true"], we do
   * "?foo=bar&debug=true".
   */
  private def buildUrlWithArgs(baseUrl: String, args: ValueRecord): String = {
    val atts = args.getAttsList.asScala

    if (atts.isEmpty) baseUrl
    else {
      val sep = if (baseUrl.contains("?")) "&" else "?"
      val encoded = atts
        .map { att =>
          val key = att.getName
          val value = att.getValue.getString.getV
          if (!att.getValue.hasString) {
            throw new DASSdkInvalidArgumentException("Column 'url_args' must be a record of string values.")
          }
          key + "=" + java.net.URLEncoder.encode(value, "UTF-8")
        }
        .mkString("&")
      s"$baseUrl$sep$encoded"
    }
  }

  private def addRequestHeaders(requestBuilder: HttpRequest.Builder, headers: ValueRecord): Unit = {
    headers.getAttsList.asScala.foreach { att =>
      val key = att.getName
      if (att.getValue.hasString) {
        val value = att.getValue.getString.getV
        requestBuilder.header(key, value)
      } else if (att.getValue.hasList) {
        val values = att.getValue.getList.getValuesList.asScala
        values.foreach { v =>
          if (!v.hasString) {
            throw new DASSdkInvalidArgumentException(
              "Column 'request_headers' must be a record of strings or a record of list of strings.")
          }

          requestBuilder.header(key, v.getString.getV)
        }
      } else {
        throw new DASSdkInvalidArgumentException(
          "Column 'request_headers' must be a record of strings or a record of list of strings.")
      }
    }
  }

  private def mkStringValue(s: String): Value = {
    Value.newBuilder().setString(ValueString.newBuilder().setV(s)).build()
  }

  private def headersToRecordValue(values: HttpHeaders): Value = {
    val atts = values.map().asScala.map { case (k, v) =>
      val list = ValueList.newBuilder()
      v.asScala.foreach { s =>
        list.addValues(Value.newBuilder().setString(ValueString.newBuilder().setV(s)).build())
      }
      val value = Value.newBuilder().setList(list).build()
      ValueRecordAttr.newBuilder().setName(k).setValue(value).build()
    }

    val recordBuilder = ValueRecord.newBuilder().addAllAtts(atts.asJava)
    Value.newBuilder().setRecord(recordBuilder).build()
  }

  private def mkInValue(value: Int): Value = {
    Value.newBuilder().setInt(ValueInt.newBuilder().setV(value)).build()
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
