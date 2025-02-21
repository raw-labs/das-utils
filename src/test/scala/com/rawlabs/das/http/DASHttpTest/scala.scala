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

/**
 * Basic tests for the HTTP DAS plugin.
 */
class DASHttpTest extends AnyFunSuite {

  test("HTTP DAS can fetch from a single endpoint") {
    // Example config with one table
    val config: Map[String, String] = Map(
      "nr_tables"       -> "1",
      "table0_name"     -> "net_http_request",
      "table0_url"      -> "http://httpbin.org/user-agent",
      "table0_method"   -> "GET",
      "table0_headers"  -> "User-Agent:MyTestClient,Accept:application/json"
    )

    val dasHttp = new DASHttp(config)

    // Confirm table definition
    val maybeTable = dasHttp.getTable("net_http_request")
    assert(maybeTable.isDefined, "Table net_http_request should exist")

    val tableDef = maybeTable.get.tableDefinition
    assert(tableDef.getColumnsCount == 5, "Should have 5 columns: url, method, response_status_code, request_headers, response_body")

    // Execute
    val executeResult = maybeTable.get.execute(
      quals = Seq.empty,
      columns = Seq.empty,   // all columns
      sortKeys = Seq.empty,
      maybeLimit = None
    )

    // Collect the single row
    var rows   = List.empty[ProtoRow]
    while (executeResult.hasNext) {
      rows = rows :+ executeResult.next()
    }
    executeResult.close()

    // Expect exactly 1 row
    assert(rows.size == 1, s"Expected 1 row, got ${rows.size}")

    val row = rows.head
    val columnsList = row.getColumnsList

    // Just check that certain columns are present
    val colNames = columnsList.toArray.map(_.asInstanceOf[com.rawlabs.protocol.das.v1.tables.Column].getName)
    assert(colNames.contains("url"),                  "Must have column url")
    assert(colNames.contains("method"),               "Must have column method")
    assert(colNames.contains("response_status_code"), "Must have column response_status_code")
    assert(colNames.contains("request_headers"),      "Must have column request_headers")
    assert(colNames.contains("response_body"),        "Must have column response_body")
  }
}