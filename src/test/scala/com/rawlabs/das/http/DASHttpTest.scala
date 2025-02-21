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

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import com.rawlabs.protocol.das.v1.query.{Operator, Qual => ProtoQual, SimpleQual}
import com.rawlabs.protocol.das.v1.tables.{Row => ProtoRow}
import com.rawlabs.protocol.das.v1.types.{Value => ProtoValue, ValueString}

/**
 * Basic tests for the single-table HTTP DAS plugin that uses Protobuf-based Quals.
 */
class DASHttpTest extends AnyFunSuite {

  test("No WHERE => defaults to GET http://httpbin.org/get") {
    val das = new DASHttp()
    val table = das.getTable("net_http_request").get

    // No quals => using defaults
    val result = table.execute(quals = Seq.empty, columns = Seq.empty, sortKeys = Seq.empty, maybeLimit = None)

    val rows = collectRows(result)
    assert(rows.size == 1)

    val row = rows.head
    val colMap = rowColsToMap(row)

    // By default, table uses url=http://httpbin.org/get, method=GET, no headers, no body
    assert(colMap("url") == "http://httpbin.org/get")
    assert(colMap("method") == "GET")
  }

  test("Set 'url' with a SimpleQual EQUALS => GET custom URL") {
    val das = new DASHttp()
    val table = das.getTable("net_http_request").get

    // Build a protobuf Qual for: url = 'http://httpbin.org/uuid'
    val urlQual = ProtoQual
      .newBuilder()
      .setName("url")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(ProtoValue
            .newBuilder()
            .setString(ValueString.newBuilder().setV("http://httpbin.org/uuid"))))
      .build()

    val quals = Seq(urlQual)

    val result = table.execute(
      quals = quals,
      columns = Seq("url", "method", "response_status_code", "response_body"),
      sortKeys = Seq.empty,
      maybeLimit = None)

    val rows = collectRows(result)
    assert(rows.size == 1)

    val row = rows.head
    val colMap = rowColsToMap(row)
    assert(colMap("url") == "http://httpbin.org/uuid")
    assert(colMap("method") == "GET") // default if not specified
  }

  test("Set 'url', 'method' = POST, and a custom request_body") {
    val das = new DASHttp()
    val table = das.getTable("net_http_request").get

    val urlQual = simpleStringQual("url", Operator.EQUALS, "http://httpbin.org/post")
    val methodQual = simpleStringQual("method", Operator.EQUALS, "POST")
    val bodyQual = simpleStringQual("request_body", Operator.EQUALS, """{"test":123}""")

    val quals = Seq(urlQual, methodQual, bodyQual)

    val result = table.execute(
      quals = quals,
      columns = Seq("url", "method", "request_body", "response_status_code", "response_body"),
      sortKeys = Seq.empty,
      maybeLimit = None)

    val rows = collectRows(result)
    assert(rows.size == 1)

    val row = rows.head
    val colMap = rowColsToMap(row)
    assert(colMap("url") == "http://httpbin.org/post")
    assert(colMap("method") == "POST")
    assert(colMap("request_body") == """{"test":123}""")
    // The response_body from httpbin should echo back our posted JSON in many cases.
  }

  test("Unsupported operator => ignored param => default used") {
    val das = new DASHttp()
    val table = das.getTable("net_http_request").get

    // Suppose we do url != 'something' (NOT_EQUALS)
    // Our parseQuals only handles EQUALS => we ignore this => default URL is used
    val urlQual = ProtoQual
      .newBuilder()
      .setName("url")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.NOT_EQUALS)
          .setValue(ProtoValue
            .newBuilder()
            .setString(ValueString.newBuilder().setV("http://fake.com"))))
      .build()

    val quals = Seq(urlQual)
    val result = table.execute(quals, Seq.empty, Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val row = rows.head
    val colMap = rowColsToMap(row)
    // Because NOT_EQUALS is not recognized => default
    assert(colMap("url") == "http://httpbin.org/get")
  }

  test("Ignoring IS_ANY_QUAL => still default") {
    // If the user does: url IN ('x','y'), we might get an IsAnyQual. Our code ignores it => default
    val das = new DASHttp()
    val table = das.getTable("net_http_request").get

    // Build an IsAnyQual for demonstration (not used, but let's see if the code just ignores it)
    // NB: We won't elaborate too much, just verifying we do not crash
    //   and the result is the default URL again.
    val qualWithIsAny = ProtoQual
      .newBuilder()
      .setName("url")
      .setIsAnyQual( // we ignore it in parseQuals
        com.rawlabs.protocol.das.v1.query.IsAnyQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .addValues(ProtoValue
            .newBuilder()
            .setString(ValueString.newBuilder().setV("http://somewhere.org"))))
      .build()

    val result = table.execute(Seq(qualWithIsAny), Seq.empty, Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val row = rows.head
    val colMap = rowColsToMap(row)
    // Still default
    assert(colMap("url") == "http://httpbin.org/get")
  }

  //
  // Helper: Build a SimpleQual for column=stringValue with operator=EQUALS
  //
  private def simpleStringQual(colName: String, op: Operator, value: String): ProtoQual = {
    ProtoQual
      .newBuilder()
      .setName(colName)
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(op)
          .setValue(ProtoValue.newBuilder().setString(ValueString.newBuilder().setV(value))))
      .build()
  }

  // Collect all rows from an ExecuteResult
  private def collectRows(result: com.rawlabs.das.sdk.DASExecuteResult): Seq[ProtoRow] = {
    val buf = scala.collection.mutable.ArrayBuffer.empty[ProtoRow]
    while (result.hasNext) {
      buf += result.next()
    }
    result.close()
    buf.toSeq
  }

  // Convert a ProtoRow's columns into a Map[String, String] for convenience
  private def rowColsToMap(row: ProtoRow): Map[String, String] = {
    row.getColumnsList.asScala.map { c =>
      val colName = c.getName
      val v = c.getData
      val strVal = if (v.hasString) v.getString.getV else ""
      colName -> strVal
    }.toMap
  }
}
