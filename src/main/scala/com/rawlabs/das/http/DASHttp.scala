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

import com.rawlabs.das.sdk.scala.{ DASSdk, DASFunction }
import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.tables.TableDefinition
import com.typesafe.scalalogging.StrictLogging

/**
 * A high-level manager for multiple HTTP tables (endpoints).
 */
class DASHttp(options: Map[String, String])
  extends DASSdk
    with StrictLogging {

  // Parse config
  private val httpOptions = new DASHttpOptions(options)

  // Create connector (manages HttpClient).
  private val connector = new DASHttpConnector()

  // Build each table from the config
  private val allTables: Seq[DASHttpTable] =
    httpOptions.tableConfigs.map { cfg =>
      new DASHttpTable(connector, cfg)
    }

  // Precompute table definitions
  private val definitions: Seq[TableDefinition] = allTables.map(_.tableDefinition)

  /** Return the list of known table definitions for this DAS. */
  override def tableDefinitions: Seq[TableDefinition] = definitions

  /** We have no function definitions. */
  override def functionDefinitions: Seq[FunctionDefinition] = Seq.empty

  /** Lookup by name. Return the matching table if any. */
  override def getTable(name: String) = allTables.find(_.httpConfig.name == name)

  /** We don’t provide any custom functions, so always None. */
  override def getFunction(name: String): Option[DASFunction] = None

}