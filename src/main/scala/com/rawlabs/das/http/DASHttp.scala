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

import com.rawlabs.das.sdk.scala.{DASFunction, DASSdk}
import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.tables.TableDefinition

/**
 * This DAS has exactly one table: "net_http_request". All query parameters (url, method, etc.) come from the WHERE
 * clause.
 */
class DASHttp extends DASSdk {

  // Create a single table instance
  private val httpTable = new DASHttpTable()

  // Return the single table definition
  override def tableDefinitions: Seq[TableDefinition] = Seq(httpTable.tableDefinition)

  // No custom functions in this plugin
  override def functionDefinitions: Seq[FunctionDefinition] = Seq.empty

  /**
   * If the user queries "net_http_request", return our single table. Otherwise, None.
   */
  override def getTable(name: String): Option[DASHttpTable] =
    if (name == "http_request") Some(httpTable) else None

  // No functions to retrieve
  override def getFunction(name: String): Option[DASFunction] = None
}
