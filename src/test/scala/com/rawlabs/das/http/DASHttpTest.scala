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

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import com.rawlabs.das.sdk.DASSdkException
import com.rawlabs.protocol.das.v1.query.{Operator, Qual => ProtoQual, SimpleQual}
import com.rawlabs.protocol.das.v1.tables.{Row => ProtoRow}
import com.rawlabs.protocol.das.v1.types.{Value => ProtoValue, ValueString}

class DASHttpTest extends AnyFunSuite {

  test("Missing 'url' => throws DASSdkException") {
    val das = new DASHttp()
    val table = das.getTable("net_http_request").get

    // No Quals => 'url' not defined => must throw
    assertThrows[DASSdkException] {
      table.execute(quals = Seq.empty, columns = Seq.empty, sortKeys = Seq.empty, maybeLimit = None)
    }
  }

  test("Specify 'url' => GET request on that URL") {
    val das = new DASHttp()
    val table = das.getTable("net_http_request").get

    // Build a Qual: url = "https://httpbin.org/uuid"
    val urlQual = ProtoQual
      .newBuilder()
      .setName("url")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(ProtoValue
            .newBuilder()
            .setString(ValueString.newBuilder().setV("https://httpbin.org/uuid"))))
      .build()

    val result = table.execute(quals = Seq(urlQual), columns = Seq.empty, sortKeys = Seq.empty, maybeLimit = None)

    val rows = collectRows(result)
    assert(rows.size == 1)

    val row = rows.head
    val colMap = rowColsToMap(row)
    assert(colMap("url") == "https://httpbin.org/uuid")
    assert(colMap("method") == "GET") // default if not specified
  }

  test("POST with custom body and header") {
    val das = new DASHttp()
    val table = das.getTable("net_http_request").get

    val urlQual = simpleStringQual("url", Operator.EQUALS, "https://httpbin.org/post")
    val methodQual = simpleStringQual("method", Operator.EQUALS, "POST")
    val headerQual = simpleStringQual("request_headers", Operator.EQUALS, "Content-Type:application/json")
    val bodyQual = simpleStringQual("request_body", Operator.EQUALS, """{"hello":"world"}""")

    val quals = Seq(urlQual, methodQual, headerQual, bodyQual)

    val result = table.execute(quals, Seq.empty, Seq.empty, None)
    val rows = collectRows(result)
    assert(rows.size == 1)

    val row = rows.head
    val colMap = rowColsToMap(row)
    assert(colMap("url") == "https://httpbin.org/post")
    assert(colMap("method") == "POST")
    assert(colMap("request_headers") == "Content-Type:application/json")
    assert(colMap("request_body") == """{"hello":"world"}""")
  }

  test("Unsupported operator => param ignored => url not found => throw exception") {
    val das = new DASHttp()
    val table = das.getTable("net_http_request").get

    // Attempt: url <> 'somewhere' => we only handle EQUALS => so 'url' remains unset => throws
    val qual = ProtoQual
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

    assertThrows[DASSdkException] {
      table.execute(Seq(qual), Seq.empty, Seq.empty, None)
    }
  }

  //
  // Helpers
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

  private def collectRows(result: com.rawlabs.das.sdk.DASExecuteResult): Seq[ProtoRow] = {
    val buf = scala.collection.mutable.ArrayBuffer.empty[ProtoRow]
    while (result.hasNext) { buf += result.next() }
    result.close()
    buf.toSeq
  }

  private def rowColsToMap(row: ProtoRow): Map[String, String] = {
    row.getColumnsList.asScala.map { col =>
      val name = col.getName
      val v = col.getData
      val str = if (v.hasString) v.getString.getV else ""
      name -> str
    }.toMap
  }
}
