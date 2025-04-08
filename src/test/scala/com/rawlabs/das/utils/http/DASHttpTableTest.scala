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

import java.net.http.HttpResponse.BodyHandler
import java.net.http._
import java.net.{ConnectException, UnknownHostException}
import javax.net.ssl.SSLException

import scala.jdk.CollectionConverters._

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{reset, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSdkUnsupportedException}
import com.rawlabs.protocol.das.v1.query.{Operator, Qual => ProtoQual, SimpleQual}
import com.rawlabs.protocol.das.v1.tables.{Row => ProtoRow}
import com.rawlabs.protocol.das.v1.types.{
  Value => ProtoValue,
  ValueBool,
  ValueInt,
  ValueList,
  ValueRecord,
  ValueRecordAttr,
  ValueString
}

/**
 * Unit tests for DASHttpTable which:
 *   - Requires 'url' in WHERE
 *   - Enforces type checking for columns
 *   - Throws if unrecognized column or operator != EQUALS
 *   - Produces 1 row with columns: response_status_code, response_body, etc.
 */
class DASHttpTableTest extends AnyFunSuite with BeforeAndAfterEach {

  private val mockClient = mock[HttpClient]
  private val mockHttpResponse = mock[HttpResponse[String]]
  private val mockHttpHeaders = mock[HttpHeaders]

  private val mockHttpTable = new DASHttpTable {
    override def buildHttpClient(followRedirect: Boolean, connectTimeoutMillis: Int, sslTrustAll: Boolean): HttpClient =
      mockClient
  }

  // Resetting mocks before each test
  override def beforeEach(): Unit = {
    reset(mockClient)
    reset(mockHttpResponse)
    reset(mockHttpHeaders)

    when(mockClient.send(any[HttpRequest], any[BodyHandler[String]]())).thenReturn(mockHttpResponse)
    when(mockHttpResponse.statusCode()).thenReturn(200)
    when(mockHttpResponse.body()).thenReturn("")
    when(mockHttpResponse.headers()).thenReturn(mockHttpHeaders)
    when(mockHttpHeaders.map()).thenReturn(Map.empty[String, java.util.List[String]].asJava)
  }

  // --------------------------------------------------------------------------
  // Tests
  // --------------------------------------------------------------------------
  test("GET method with args => success") {
    when(mockHttpResponse.statusCode()).thenReturn(203)
    when(mockHttpResponse.body()).thenReturn("some body")
    // I want to get the headers as a Map[String, List[String]] so returning 2 values
    val headers = Map[String, java.util.List[String]]("foo" -> List("bar", "buzz").asJava)
    when(mockHttpHeaders.map()).thenReturn(headers.asJava)

    val qMethod = qualString("method", "GET")
    val qUrl = qualString("url", "https://example.com/get")
    // get method without args is returning a 502 error
    val qArgs = qualRecord("url_args", Map("debug" -> "true", "test" -> "123"))

    val quals = Seq(qUrl, qMethod, qArgs)
    val result = mockHttpTable.execute(
      quals,
      Seq("method", "response_status_code", "response_body", "response_headers"),
      Seq.empty,
      None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("method") == "GET")
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "203")
    assert(rowMap("response_body") == "some body")
    assert(rowMap("response_headers") == "{foo:[bar, buzz]}")
  }

  test("GET with request header with list of strings => success") {
    val qUrl = qualString("url", "https://example.com/get")

    val qHeaders =
      qualRecord("request_headers", Map("simple" -> "foo", "repeated" -> List("foo", "bar")))

    val quals = Seq(qUrl, qHeaders)

    val result = mockHttpTable.execute(quals, Seq("url", "request_headers"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    // The table might append "?debug=true&test=123" to the URL
    assert(rowMap("url") == "https://example.com/get")
    assert(rowMap("request_headers") == "{simple:foo, repeated:[foo, bar]}")
  }

  test("POST with custom headers & url_args => success") {
    val qUrl = qualString("url", "https://example.com/post")
    val qMethod = qualString("method", "POST")
    val qBody = qualString("request_body", """{"foo":"bar"}""")
    val qRedirect = qualBool("follow_redirects", boolVal = true)

    val qHeaders =
      qualRecord("request_headers", Map("Content-Type" -> "application/json", "User-Agent" -> "MyDAS"))
    val qArgs = qualRecord("url_args", Map("debug" -> "true", "test" -> "123"))

    val quals = Seq(qUrl, qMethod, qBody, qRedirect, qHeaders, qArgs)

    val result = mockHttpTable.execute(
      quals,
      Seq("url", "request_headers", "url_args", "follow_redirects", "response_status_code", "response_body"),
      Seq.empty,
      None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    // The table might append "?debug=true&test=123" to the URL
    assert(rowMap("url") == "https://example.com/post")
    assert(rowMap("follow_redirects") == "true")
    assert(rowMap("request_headers") == "{Content-Type:application/json, User-Agent:MyDAS}")
    assert(rowMap("url_args") == "{debug:true, test:123}")
    assert(rowMap("response_status_code") == "200")
    assert(rowMap("response_body") == "")
  }

  test("DELETE method => success") {
    val qMethod = qualString("method", "DELETE")
    val qUrl = qualString("url", "https://example.com/delete")

    val quals = Seq(qUrl, qMethod)
    val result = mockHttpTable.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("method") == "DELETE")
    // Possibly 200 or 204 if the endpoint simulates DELETE
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "200")
    assert(rowMap.contains("response_body"))
  }

  test("POST method => success") {
    val qMethod = qualString("method", "POST")
    val qUrl = qualString("url", "https://example.com/post")
    val qBody = qualString("request_body", """{"foo":"bar"}""")

    val quals = Seq(qUrl, qMethod, qBody)
    val result = mockHttpTable.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("method") == "POST")
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "200")
    assert(rowMap.contains("response_body"))
  }

  test("PUT method => success") {
    val qMethod = qualString("method", "PUT")
    val qUrl = qualString("url", "https://example.com/put")
    val qBody = qualString("request_body", """{"foo":"bar"}""")
    // put method without args is returning a 502 error
    val qArgs = qualRecord("url_args", Map("debug" -> "true", "test" -> "123"))

    val quals = Seq(qUrl, qMethod, qBody, qArgs)
    val result = mockHttpTable.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("method") == "PUT")
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "200")
    assert(rowMap.contains("response_body"))
  }

  test("PATCH method => success") {
    val qMethod = qualString("method", "PATCH")
    val qUrl = qualString("url", "https://example.com/patch")
    val qBody = qualString("request_body", """{"foo":"bar"}""")

    val quals = Seq(qUrl, qMethod, qBody)
    val result = mockHttpTable.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("method") == "PATCH")
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "200")
    assert(rowMap.contains("response_body"))
  }

  test("HEAD method => success") {
    val qMethod = qualString("method", "HEAD")
    val qUrl = qualString("url", "https://example.com/anything")

    val quals = Seq(qUrl, qMethod)
    val result = mockHttpTable.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("method") == "HEAD")
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "200")
    assert(rowMap.contains("response_body"))
  }

  test("OPTIONS method => success") {
    val qMethod = qualString("method", "OPTIONS")
    val qUrl = qualString("url", "https://example.com/anything")
    val qBody = qualString("request_body", """{"foo":"bar"}""")

    val quals = Seq(qUrl, qMethod, qBody)
    val result = mockHttpTable.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("method") == "OPTIONS")
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "200")
    assert(rowMap.contains("response_body"))
  }

  test("follow_redirects default is true") {
    val qUrl = qualString("url", "http://example.com")
    val quals = Seq(qUrl)
    val result = mockHttpTable.execute(quals, Seq("follow_redirects"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("follow_redirects") == "true")
  }

  test("Missing 'url' => throws DASSdkException") {
    val quals = Seq(
      qualString("method", "GET") // no url => should throw
    )

    val ex = intercept[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(quals, Seq.empty, Seq.empty, None)
    }

    assert(ex.getMessage.contains("Missing 'url'"))
  }

  test("malformed 'url' => throws DASSdkInvalidArgumentException") {
    val quals = Seq(
      qualString("url", "not and url") // malformed url => should throw
    )

    assertThrows[DASSdkInvalidArgumentException] {
      val res = mockHttpTable.execute(quals, Seq.empty, Seq.empty, None)
      collectRows(res)
    }
  }

  test("not supported scheme 'url' => throws DASSdkInvalidArgumentException") {
    val quals = Seq(
      qualString("url", "file://path/file") // not supported scheme url => should throw
    )

    assertThrows[DASSdkInvalidArgumentException] {
      val res = mockHttpTable.execute(quals, Seq.empty, Seq.empty, None)
      collectRows(res)
    }
  }

  test("Wrong operator => throws DASSdkInvalidArgumentException") {
    // We'll build a Qual with operator = LESS_THAN
    val q = ProtoQual
      .newBuilder()
      .setName("url")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.LESS_THAN) // not EQUALS => should throw
          .setValue(ProtoValue
            .newBuilder()
            .setString(ValueString.newBuilder().setV("http://example.com"))))
      .build()

    assertThrows[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(Seq(q), Seq.empty, Seq.empty, None)
    }
  }

  test("follow_redirects is not a bool => throws DASSdkInvalidArgumentException") {

    val qUrl = qualString("url", "http://example.com")
    val qMethod = qualString("method", "GET")
    // follow_redirects => string "true" => mismatch
    val qRedirect = qualString("follow_redirects", "true")

    val quals = Seq(qUrl, qMethod, qRedirect)
    assertThrows[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(quals, Seq.empty, Seq.empty, None)
    }
  }

  test("request_headers is not a record => throws DASSdkInvalidArgumentException") {

    val qUrl = qualString("url", "http://example.com")
    val qMethod = qualString("method", "GET")
    // request_headers => string => mismatch
    val qHeaders = qualString("request_headers", "Accept:application/json")

    val quals = Seq(qUrl, qMethod, qHeaders)
    assertThrows[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(quals, Seq.empty, Seq.empty, None)
    }
  }

  test("Unknown method => throw DASSdkInvalidArgumentException") {
    val qMethod = qualString("method", "FOOBAR")
    val qUrl = qualString("url", "https://example.com/anything")

    val quals = Seq(qUrl, qMethod)
    assertThrows[DASSdkInvalidArgumentException] {
      val result = mockHttpTable.execute(quals, Seq.empty, Seq.empty, None)
      collectRows(result)
    }
  }

  test("Qual is not a simpleQual => throws DASSdkInvalidArgumentException") {
    // Create a Qual that does NOT have a simpleQual (so q.hasSimpleQual == false)
    val invalidQual = ProtoQual
      .newBuilder()
      .setName("url")
      // Deliberately do NOT set .setSimpleQual(...)
      .build()

    val ex = intercept[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(Seq(invalidQual), Seq.empty, Seq.empty, None)
    }

    assert(ex.getMessage.contains("Only EQUALS operator is supported for column"))
  }

  test("request_timeout_millis is not int => throws DASSdkInvalidArgumentException") {
    // Provide a string value instead of an int
    val qUrl = qualString("url", "http://example.com")
    val qTimeout = qualString("request_timeout_millis", "notAnInt")

    val ex = intercept[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(Seq(qUrl, qTimeout), Seq.empty, Seq.empty, None)
    }
    assert(ex.getMessage.contains("must be an integer value"))
  }

  test("ssl_trust_all is not bool => throws DASSdkInvalidArgumentException") {
    val qUrl = qualString("url", "http://example.com")
    val qSslTrustAll = qualString("ssl_trust_all", "trueInsteadOfBool")

    val ex = intercept[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(Seq(qUrl, qSslTrustAll), Seq.empty, Seq.empty, None)
    }
    assert(ex.getMessage.contains("must be a boolean value"))
  }

  test("request_body is not string => throws DASSdkInvalidArgumentException") {
    val qUrl = qualString("url", "http://example.com")
    // Provide a bool instead of a string
    val qBody = qualBool("request_body", boolVal = true)

    val ex = intercept[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(Seq(qUrl, qBody), Seq.empty, Seq.empty, None)
    }
    assert(ex.getMessage.contains("must be a string value"))
  }

  test("method is not string => throws DASSdkInvalidArgumentException") {
    val qUrl = qualString("url", "http://example.com")
    // Provide a bool instead of a string
    val qMethod = qualBool("method", boolVal = true)

    val ex = intercept[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(Seq(qUrl, qMethod), Seq.empty, Seq.empty, None)
    }
    assert(ex.getMessage.contains("must be a string value"))
  }

  test("UnknownHostException => throws DASSdkInvalidArgumentException") {
    // Force the client.send() to throw UnknownHostException
    when(mockClient.send(any[java.net.http.HttpRequest], any[BodyHandler[String]]()))
      .thenThrow(new UnknownHostException("no-such-host"))

    val qUrl = qualString("url", "http://some-unknown-host")
    val ex = intercept[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(Seq(qUrl), Seq.empty, Seq.empty, None)
    }
    assert(ex.getMessage.contains("Unknown host: no-such-host"))
  }

  test("HttpTimeoutException => throws DASSdkInvalidArgumentException") {
    val qMethod = qualString("method", "GET")
    val qUrl = qualString("url", "https://example.com/get")

    when(mockClient.send(any[HttpRequest], any[HttpResponse.BodyHandler[String]]))
      .thenThrow(new HttpTimeoutException("Request timed out"))

    val qConnectTimeout = qualInt("connect_timeout_millis", 1)

    val quals = Seq(qUrl, qMethod, qConnectTimeout)
    val ex = intercept[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
    }

    assert(ex.getMessage.contains("Request timed out:"))
  }

  test("ConnectException => throws DASSdkInvalidArgumentException") {
    when(mockClient.send(any[java.net.http.HttpRequest], any[BodyHandler[String]]()))
      .thenThrow(new ConnectException("Connection refused"))

    val qUrl = qualString("url", "http://connection-refused.com")
    val ex = intercept[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(Seq(qUrl), Seq.empty, Seq.empty, None)
    }
    assert(ex.getMessage.contains("Connection error: Connection refused"))
  }

  test("SSLException => throws DASSdkInvalidArgumentException") {
    when(mockClient.send(any[java.net.http.HttpRequest], any[BodyHandler[String]]()))
      .thenThrow(new SSLException("Untrusted certificate"))

    val qUrl = qualString("url", "https://example.com/ssl")
    val ex = intercept[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(Seq(qUrl), Seq.empty, Seq.empty, None)
    }
    assert(ex.getMessage.contains("SSL error: Untrusted certificate"))
  }

  test("IOException => throws DASSdkInvalidArgumentException") {
    // This also covers other I/O issues
    when(mockClient.send(any[java.net.http.HttpRequest], any[BodyHandler[String]]()))
      .thenThrow(new java.io.IOException("Some IO error"))

    val qUrl = qualString("url", "https://example.com/io")
    val ex = intercept[DASSdkInvalidArgumentException] {
      mockHttpTable.execute(Seq(qUrl), Seq.empty, Seq.empty, None)
    }
    assert(ex.getMessage.contains("Network I/O error: Some IO error"))
  }

  // Tests for the read-only operations: insert, update, delete

  test("insert => throws DASSdkInvalidArgumentException") {
    val row = ProtoRow.getDefaultInstance
    intercept[DASSdkUnsupportedException] {
      mockHttpTable.insert(row)
    }
  }

  test("update => throws DASSdkInvalidArgumentException") {
    val row = ProtoRow.getDefaultInstance
    intercept[DASSdkUnsupportedException] {
      mockHttpTable.update(ProtoValue.newBuilder().build(), row)
    }
  }

  test("delete => throws DASSdkInvalidArgumentException") {
    intercept[DASSdkUnsupportedException] {
      mockHttpTable.delete(ProtoValue.newBuilder().build())
    }
  }

  // A helper method to create a SimpleQual with (column = stringValue)
  private def qualString(colName: String, strVal: String): ProtoQual = {
    val strValue = ProtoValue
      .newBuilder()
      .setString(ValueString.newBuilder().setV(strVal))

    ProtoQual
      .newBuilder()
      .setName(colName)
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(strValue))
      .build()
  }

  // A helper method to create a SimpleQual with (column = boolValue)
  private def qualBool(colName: String, boolVal: Boolean): ProtoQual = {
    val boolValue = ProtoValue
      .newBuilder()
      .setBool(ValueBool.newBuilder().setV(boolVal))

    ProtoQual
      .newBuilder()
      .setName(colName)
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(boolValue))
      .build()
  }

  // A helper method to create a SimpleQual with (column = boolValue)
  private def qualInt(colName: String, intVal: Int): ProtoQual = {
    val boolValue = ProtoValue
      .newBuilder()
      .setInt(ValueInt.newBuilder().setV(intVal))

    ProtoQual
      .newBuilder()
      .setName(colName)
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(boolValue))
      .build()
  }

  // A helper method to build a record of string key->value for "request_headers" or "url_args"
  // For example: request_headers = { "Content-Type" : "application/json", "X-Foo" : "Bar" }
  private def qualRecord(colName: String, kvPairs: Map[String, Any]): ProtoQual = {
    val recordBuilder = ValueRecord.newBuilder()
    kvPairs.foreach {
      case (k, v: String) =>
        val valAttr = ValueRecordAttr
          .newBuilder()
          .setName(k)
          .setValue(ProtoValue.newBuilder().setString(ValueString.newBuilder().setV(v)))
        recordBuilder.addAtts(valAttr)
      case (k, v: List[_]) =>
        val values = v.map(s => ProtoValue.newBuilder().setString(ValueString.newBuilder().setV(s.toString)).build())
        val valAttr = ValueRecordAttr
          .newBuilder()
          .setName(k)
          .setValue(
            ProtoValue
              .newBuilder()
              .setList(ValueList.newBuilder().addAllValues(values.asJava)))

        recordBuilder.addAtts(valAttr)
      case _ => throw new IllegalArgumentException("Only string or list of strings are supported")
    }
    val recordValue = ProtoValue.newBuilder().setRecord(recordBuilder)

    ProtoQual
      .newBuilder()
      .setName(colName)
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(recordValue))
      .build()
  }

  // Helper: gather all rows from DASExecuteResult
  private def collectRows(result: com.rawlabs.das.sdk.DASExecuteResult): Seq[ProtoRow] = {
    val buf = scala.collection.mutable.ArrayBuffer.empty[ProtoRow]
    while (result.hasNext) {
      buf += result.next()
    }
    result.close()
    buf.toSeq
  }

  // Helper: convert a row into a Map(colName -> stringValue) just for test checks
  private def rowToMap(row: ProtoRow): Map[String, String] = {
    row.getColumnsList.asScala.map { c =>
      c.getName -> valueToString(c.getData)
    }.toMap
  }

  private def valueToString(v: ProtoValue): String = {
    if (v.hasString) v.getString.getV
    else if (v.hasBool) v.getBool.getV.toString
    else if (v.hasInt) v.getInt.getV.toString
    else if (v.hasRecord) {
      v.getRecord.getAttsList.asScala
        .map { a =>
          a.getName + ":" + valueToString(a.getValue)
        }
    }.mkString("{", ", ", "}")
    else if (v.hasList) {
      v.getList.getValuesList.asScala
        .map(valueToString)
        .mkString("[", ", ", "]")
    } else s"<complex or unsupported: $v>"
  }

}
