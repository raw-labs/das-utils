package com.rawlabs.das.http

import org.scalatest.funsuite.AnyFunSuite

import com.rawlabs.protocol.das.v1.tables.{Row => ProtoRow}
import com.rawlabs.protocol.das.v1.query.{
  Qual => ProtoQual,
  SimpleQual,
  Operator
}
import com.rawlabs.protocol.das.v1.types.{
  Value => ProtoValue,
  ValueString,
  ValueBool,
  ValueRecord,
  ValueRecordAttr
}
import com.rawlabs.das.sdk.DASSdkException

import scala.jdk.CollectionConverters._

/**
 * Unit tests for DASHttpTable which:
 *  - Requires 'url' in WHERE
 *  - Enforces type checking for columns
 *  - Throws if unrecognized column or operator != EQUALS
 *  - Produces 1 row with columns: response_status_code, response_body, etc.
 */
class DASHttpTableTest extends AnyFunSuite {

  // A helper method to create a SimpleQual with (column = stringValue)
  private def qualString(colName: String, strVal: String): ProtoQual = {
    val strValue = ProtoValue.newBuilder()
      .setString(ValueString.newBuilder().setV(strVal))

    ProtoQual.newBuilder()
      .setName(colName)
      .setSimpleQual(
        SimpleQual.newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(strValue)
      )
      .build()
  }

  // A helper method to create a SimpleQual with (column = boolValue)
  private def qualBool(colName: String, boolVal: Boolean): ProtoQual = {
    val boolValue = ProtoValue.newBuilder()
      .setBool(ValueBool.newBuilder().setV(boolVal))

    ProtoQual.newBuilder()
      .setName(colName)
      .setSimpleQual(
        SimpleQual.newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(boolValue)
      )
      .build()
  }

  // A helper method to build a record of string key->value for "request_headers" or "url_args"
  // For example: request_headers = { "Content-Type" : "application/json", "X-Foo" : "Bar" }
  private def qualRecordOfStrings(colName: String, kvPairs: Map[String, String]): ProtoQual = {
    val recordBuilder = ValueRecord.newBuilder()
    kvPairs.foreach { case (k, v) =>
      val valAttr = ValueRecordAttr.newBuilder()
        .setName(k)
        .setValue(
          ProtoValue.newBuilder().setString(
            ValueString.newBuilder().setV(v)
          )
        )
      recordBuilder.addAtts(valAttr)
    }
    val recordValue = ProtoValue.newBuilder().setRecord(recordBuilder)

    ProtoQual.newBuilder()
      .setName(colName)
      .setSimpleQual(
        SimpleQual.newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(recordValue)
      )
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
      val v    = c.getData
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
    val quals = Seq(
      qualString("url", "http://example.com"),
      qualString("method", "GET"),
      qualBool("some_unknown_col", true)
    )

    assertThrows[DASSdkException] {
      table.execute(quals, Seq.empty, Seq.empty, None)
    }
  }

  test("Wrong operator => throws DASSdkException") {
    val table = new DASHttpTable()
    // We'll build a Qual with operator = LESS_THAN
    val q = ProtoQual.newBuilder()
      .setName("url")
      .setSimpleQual(
        SimpleQual.newBuilder()
          .setOperator(Operator.LESS_THAN)  // not EQUALS => should throw
          .setValue(
            ProtoValue.newBuilder()
              .setString(ValueString.newBuilder().setV("http://example.com"))
          )
      )
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

  test("Valid GET with minimal parameters => returns one row") {
    val table = new DASHttpTable()
    val qUrl = qualString("url", "https://httpbin.org/get")
    val qMethod = qualString("method", "GET")
    val qBody = qualString("request_body", "")
    val qRedirect = qualBool("follow_redirect", false)
    val qHeaders = qualRecordOfStrings("request_headers", Map.empty)
    val qArgs = qualRecordOfStrings("url_args", Map.empty)

    val quals = Seq(qUrl, qMethod, qBody, qRedirect, qHeaders, qArgs)

    val result = table.execute(
      quals,
      Seq("url","method","response_status_code","response_body"),
      Seq.empty,
      None
    )
    val rows = collectRows(result)
    assert(rows.size == 1)

    val row = rows.head
    val rowMap = rowToMap(row)

    assert(rowMap("url") == "https://httpbin.org/get")
    assert(rowMap("method") == "GET")
    // we can't fully predict the status code if the call is live, but typically "200"
    assert(rowMap.contains("response_status_code"))
    assert(rowMap.contains("response_body"))
  }

  test("POST with custom headers & url_args => returns one row") {
    val table = new DASHttpTable()
    val qUrl = qualString("url", "https://httpbin.org/post")
    val qMethod = qualString("method", "POST")
    val qBody = qualString("request_body", """{"foo":"bar"}""")
    val qRedirect = qualBool("follow_redirect", true)

    val qHeaders = qualRecordOfStrings("request_headers", Map(
      "Content-Type" -> "application/json",
      "User-Agent"   -> "MyDAS"
    ))
    val qArgs = qualRecordOfStrings("url_args", Map("debug" -> "true","test"->"123"))

    val quals = Seq(qUrl, qMethod, qBody, qRedirect, qHeaders, qArgs)

    val result = table.execute(
      quals,
      Seq("url","request_headers","url_args","follow_redirect","response_status_code","response_body"),
      Seq.empty,
      None
    )
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    // The table might append "?debug=true&test=123" to the URL
    assert(rowMap("url").contains("https://httpbin.org/post"))
    assert(rowMap("follow_redirect") == "true")
    assert(rowMap.contains("request_headers"))
    assert(rowMap.contains("url_args"))
    assert(rowMap.contains("response_status_code"))
    assert(rowMap.contains("response_body"))
  }
}