package com.rawlabs.das.http

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import com.rawlabs.das.sdk.DASSdkException
import com.rawlabs.protocol.das.v1.query.{Operator, Qual => ProtoQual, SimpleQual}
import com.rawlabs.protocol.das.v1.tables.{Row => ProtoRow}
import com.rawlabs.protocol.das.v1.types.{Value => ProtoValue, ValueBool, ValueRecord, ValueRecordAttr, ValueString}

/**
 * Unit tests for DASHttpTable which:
 *   - Requires 'url' in WHERE
 *   - Enforces type checking for columns
 *   - Throws if unrecognized column or operator != EQUALS
 *   - Produces 1 row with columns: response_status_code, response_body, etc.
 */
class DASHttpTableTest extends AnyFunSuite {

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

  // A helper method to build a record of string key->value for "request_headers" or "url_args"
  // For example: request_headers = { "Content-Type" : "application/json", "X-Foo" : "Bar" }
  private def qualRecordOfStrings(colName: String, kvPairs: Map[String, String]): ProtoQual = {
    val recordBuilder = ValueRecord.newBuilder()
    kvPairs.foreach { case (k, v) =>
      val valAttr = ValueRecordAttr
        .newBuilder()
        .setName(k)
        .setValue(ProtoValue.newBuilder().setString(ValueString.newBuilder().setV(v)))
      recordBuilder.addAtts(valAttr)
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
      val name = c.getName
      val v = c.getData
      val s =
        if (v.hasString) v.getString.getV
        else if (v.hasBool) v.getBool.getV.toString
        else if (v.hasInt) v.getInt.getV.toString
        else s"<complex or unsupported: $v>"
      name -> s
    }.toMap
  }

  // --------------------------------------------------------------------------
  // Tests
  // --------------------------------------------------------------------------

  test("Missing 'url' => throws DASSdkException") {
    val table = new DASHttpTable()
    val quals = Seq(
      qualString("method", "GET") // no url => should throw
    )

    assertThrows[DASSdkException] {
      val res = table.execute(quals, Seq.empty, Seq.empty, None)
      collectRows(res)
    }
  }

  test("Unknown column => throws DASSdkException") {
    val table = new DASHttpTable()
    val quals =
      Seq(qualString("url", "http://example.com"), qualString("method", "GET"), qualBool("some_unknown_col", true))

    assertThrows[DASSdkException] {
      table.execute(quals, Seq.empty, Seq.empty, None)
    }
  }

  test("Wrong operator => throws DASSdkException") {
    val table = new DASHttpTable()
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

    assertThrows[DASSdkException] {
      table.execute(Seq(q), Seq.empty, Seq.empty, None)
    }
  }

  test("follow_redirect is not a bool => throws DASSdkException") {
    val table = new DASHttpTable()

    val qUrl = qualString("url", "http://example.com")
    val qMethod = qualString("method", "GET")
    // follow_redirect => string "true" => mismatch
    val qRedirect = qualString("follow_redirect", "true")

    val quals = Seq(qUrl, qMethod, qRedirect)
    assertThrows[DASSdkException] {
      table.execute(quals, Seq.empty, Seq.empty, None)
    }
  }

  test("request_headers is not a record => throws DASSdkException") {
    val table = new DASHttpTable()

    val qUrl = qualString("url", "http://example.com")
    val qMethod = qualString("method", "GET")
    // request_headers => string => mismatch
    val qHeaders = qualString("request_headers", "Accept:application/json")

    val quals = Seq(qUrl, qMethod, qHeaders)
    assertThrows[DASSdkException] {
      table.execute(quals, Seq.empty, Seq.empty, None)
    }
  }

  test("GET method with args => success") {
    val table = new DASHttpTable()
    val qMethod = qualString("method", "GET")
    val qUrl = qualString("url", "https://httpbin.org/get")
    // get method without args is returning a 502 error
    val qArgs = qualRecordOfStrings("url_args", Map("debug" -> "true", "test" -> "123"))

    val quals = Seq(qUrl, qMethod, qArgs)
    val result = table.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("method") == "GET")
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "200")
    assert(rowMap.contains("response_body"))
  }

  test("POST with custom headers & url_args => returns one row") {
    val table = new DASHttpTable()
    val qUrl = qualString("url", "https://httpbin.org/post")
    val qMethod = qualString("method", "POST")
    val qBody = qualString("request_body", """{"foo":"bar"}""")
    val qRedirect = qualBool("follow_redirect", true)

    val qHeaders =
      qualRecordOfStrings("request_headers", Map("Content-Type" -> "application/json", "User-Agent" -> "MyDAS"))
    val qArgs = qualRecordOfStrings("url_args", Map("debug" -> "true", "test" -> "123"))

    val quals = Seq(qUrl, qMethod, qBody, qRedirect, qHeaders, qArgs)

    val result = table.execute(
      quals,
      Seq("url", "request_headers", "url_args", "follow_redirect", "response_status_code", "response_body"),
      Seq.empty,
      None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    // The table might append "?debug=true&test=123" to the URL
    assert(rowMap("url").contains("https://httpbin.org/post"))
    assert(rowMap("follow_redirect") == "true")
    assert(rowMap.contains("request_headers"))
    assert(rowMap.contains("url_args"))
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "200")
    assert(rowMap.contains("response_body"))

  }

  test("DELETE method => success") {
    val table = new DASHttpTable()
    val qMethod = qualString("method", "DELETE")
    val qUrl = qualString("url", "https://httpbin.org/delete")

    val quals = Seq(qUrl, qMethod)
    val result = table.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
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
    val table = new DASHttpTable()
    val qMethod = qualString("method", "POST")
    val qUrl = qualString("url", "https://httpbin.org/post")
    val qBody = qualString("request_body", """{"foo":"bar"}""")

    val quals = Seq(qUrl, qMethod, qBody)
    val result = table.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("method") == "POST")
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "200")
    assert(rowMap.contains("response_body"))
  }

  test("PUT method => success") {
    val table = new DASHttpTable()
    val qMethod = qualString("method", "PUT")
    val qUrl = qualString("url", "https://httpbin.org/put")
    val qBody = qualString("request_body", """{"foo":"bar"}""")
    // put method without args is returning a 502 error
    val qArgs = qualRecordOfStrings("url_args", Map("debug" -> "true", "test" -> "123"))

    val quals = Seq(qUrl, qMethod, qBody, qArgs)
    val result = table.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("method") == "PUT")
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "200")
    assert(rowMap.contains("response_body"))
  }

  test("PATCH method => success") {
    val table = new DASHttpTable()
    val qMethod = qualString("method", "PATCH")
    val qUrl = qualString("url", "https://httpbin.org/patch")
    val qBody = qualString("request_body", """{"foo":"bar"}""")

    val quals = Seq(qUrl, qMethod, qBody)
    val result = table.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("method") == "PATCH")
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "200")
    assert(rowMap.contains("response_body"))
  }

  test("HEAD method => success") {
    val table = new DASHttpTable()
    val qMethod = qualString("method", "HEAD")
    val qUrl = qualString("url", "https://httpbin.org/anything")

    val quals = Seq(qUrl, qMethod)
    val result = table.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("method") == "HEAD")
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "200")
    assert(rowMap.contains("response_body"))
  }

  test("OPTIONS method => success") {
    val table = new DASHttpTable()
    val qMethod = qualString("method", "OPTIONS")
    val qUrl = qualString("url", "https://httpbin.org/anything")
    val qBody = qualString("request_body", """{"foo":"bar"}""")

    val quals = Seq(qUrl, qMethod, qBody)
    val result = table.execute(quals, Seq("method", "response_status_code", "response_body"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("method") == "OPTIONS")
    assert(rowMap.contains("response_status_code"))
    assert(rowMap("response_status_code") == "200")
    assert(rowMap.contains("response_body"))
  }

  test("Unknown method => throw DASSdkException") {
    val table = new DASHttpTable()
    val qMethod = qualString("method", "FOOBAR")
    val qUrl = qualString("url", "https://httpbin.org/anything")

    val quals = Seq(qUrl, qMethod)
    assertThrows[DASSdkException] {
      val result = table.execute(quals, Seq.empty, Seq.empty, None)
      collectRows(result)
    }
  }
}
