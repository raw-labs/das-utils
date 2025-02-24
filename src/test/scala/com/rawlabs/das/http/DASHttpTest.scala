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
  ValueList,
  ValueBool
}

import com.rawlabs.das.sdk.DASSdkException

import scala.jdk.CollectionConverters._

class DASHttpTest extends AnyFunSuite {

  // Helper: create a SimpleQual for col = stringValue
  private def qualString(col: String, value: String): ProtoQual = {
    ProtoQual.newBuilder()
      .setName(col)
      .setSimpleQual(
        SimpleQual.newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(
            ProtoValue.newBuilder()
              .setString(ValueString.newBuilder().setV(value))
          )
      )
      .build()
  }

  // Helper: create a SimpleQual for col = boolValue
  private def qualBool(col: String, boolVal: Boolean): ProtoQual = {
    ProtoQual.newBuilder()
      .setName(col)
      .setSimpleQual(
        SimpleQual.newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(
            ProtoValue.newBuilder()
              .setBool(ValueBool.newBuilder().setV(boolVal))
          )
      )
      .build()
  }

  // Helper: create a SimpleQual for col = listOfStrings
  private def qualListStrings(col: String, strings: Seq[String]): ProtoQual = {
    val listBuilder = ValueList.newBuilder()
    strings.foreach { s =>
      listBuilder.addValues(
        ProtoValue.newBuilder()
          .setString(ValueString.newBuilder().setV(s))
      )
    }
    val valueList = ProtoValue.newBuilder().setList(listBuilder)
    ProtoQual.newBuilder()
      .setName(col)
      .setSimpleQual(
        SimpleQual.newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(valueList)
      )
      .build()
  }

  /**
   * Helper: read all rows from the DASExecuteResult into a Seq, then close it.
   */
  private def collectRows(result: com.rawlabs.das.sdk.DASExecuteResult): Seq[ProtoRow] = {
    val buf = scala.collection.mutable.ArrayBuffer.empty[ProtoRow]
    while (result.hasNext) {
      buf += result.next()
    }
    result.close()
    buf.toSeq
  }

  /**
   * Convert a ProtoRow into Map[colName -> stringVal].
   * For lists or bool, we do a simple approximation here.
   */
  private def rowToMap(row: ProtoRow): Map[String, String] = {
    row.getColumnsList.asScala.map { col =>
      val colName = col.getName
      val v       = col.getData
      val strVal =
        if (v.hasString) {
          v.getString.getV
        } else if (v.hasBool) {
          v.getBool.getV.toString
        } else if (v.hasInt) {
          v.getInt.getV.toString
        } else if (v.hasList) {
          // Convert list of strings to e.g. "[ item1, item2 ]"
          val items = v.getList.getValuesList.asScala.map { vi =>
            if (vi.hasString) vi.getString.getV else "???"
          }
          items.mkString("[", ", ", "]")
        } else {
          // fallback
          ""
        }

      colName -> strVal
    }.toMap
  }

  // ------------------------------------------------------------------------------
  // Now the tests
  // ------------------------------------------------------------------------------

  test("Missing url => throws DASSdkException") {
    val table = new DASHttpTable()

    // We only specify method=GET, but no url => must throw
    val quals = Seq(
      qualString("method", "GET")
    )

    assertThrows[DASSdkException] {
      val result = table.execute(quals, Seq.empty, Seq.empty, None)
      collectRows(result)
    }
  }

  test("Simple GET with no headers, no args, follow_redirect=false") {
    val table = new DASHttpTable()

    val quals = Seq(
      qualString("url", "https://httpbin.org/get"),
      qualString("method", "GET"),
      // empty lists
      qualListStrings("request_headers", Nil),
      qualListStrings("url_args", Nil),
      qualString("request_body", ""),
      qualBool("follow_redirect", boolVal = false)
    )

    val result = table.execute(quals, Seq("url","follow_redirect","response_status_code","response_body"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("url") == "https://httpbin.org/get")
    assert(rowMap("follow_redirect") == "false")
    // response_status_code is an int => in the row map we have it as a string
    assert(rowMap.contains("response_status_code"))
    assert(rowMap.contains("response_body"))
  }

  test("POST with headers, url_args, follow_redirect=true") {
    val table = new DASHttpTable()

    val quals = Seq(
      qualString("url", "https://httpbin.org/post"),
      qualString("method", "POST"),
      // e.g. Content-Type, X-Test
      qualListStrings("request_headers", Seq("Content-Type:application/json", "X-Test:Foo")),
      // e.g. foo=bar, debug=true
      qualListStrings("url_args", Seq("foo=bar","debug=true")),
      qualString("request_body", """{"hello":"world"}"""),
      qualBool("follow_redirect", boolVal = true)
    )

    // Request a few columns
    val result = table.execute(
      quals,
      Seq("url","method","follow_redirect","request_headers","url_args","response_status_code","response_body"),
      Seq.empty,
      None
    )
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("url") == "https://httpbin.org/post")  // might be appended by buildUrlWithArgs
    assert(rowMap("method") == "POST")
    assert(rowMap("follow_redirect") == "true")
    // request_headers => "[Content-Type:application/json, X-Test:Foo]"
    assert(rowMap("request_headers").contains("Content-Type:application/json"))
    assert(rowMap("request_headers").contains("X-Test:Foo"))
    // url_args => "[foo=bar, debug=true]"
    assert(rowMap("url_args").contains("foo=bar"))
    assert(rowMap("url_args").contains("debug=true"))
    // response_status_code, response_body => not checked in detail, just that they exist
    assert(rowMap.contains("response_status_code"))
    assert(rowMap.contains("response_body"))
  }

  test("Ignore unknown columns => default remains") {
    val table = new DASHttpTable()

    // We'll do "some_unknown_col = 123" => the plugin should ignore it.
    val quals = Seq(
      qualString("url", "https://httpbin.org/get"),
      qualString("method", "GET"),
      qualListStrings("request_headers", Nil),
      qualListStrings("url_args", Nil),
      qualString("request_body", ""),
      qualBool("follow_redirect", boolVal = false),
      ProtoQual.newBuilder()
        .setName("some_unknown_col")
        .setSimpleQual(
          SimpleQual.newBuilder()
            .setOperator(Operator.EQUALS)
            .setValue(ProtoValue.newBuilder().setString(ValueString.newBuilder().setV("123")))
        )
        .build()
    )

    val result = table.execute(quals, Seq("url","follow_redirect","response_status_code"), Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val rowMap = rowToMap(rows.head)
    assert(rowMap("url") == "https://httpbin.org/get")
    assert(rowMap("follow_redirect") == "false")
    assert(rowMap.contains("response_status_code"))
    // "some_unknown_col" not used => no error
  }

}