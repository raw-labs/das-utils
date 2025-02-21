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

import scala.util.Try

/**
 * Configuration for a single “table” i.e. one HTTP endpoint.
 */
case class HttpTableConfig(
    name: String, // Name of the table
    url: String, // The HTTP URL to fetch
    method: String, // GET/POST/etc.
    headers: Map[String, String] // Key-value pairs for request headers
)

/**
 * Holds all parsed config for the HTTP DAS. Example config keys might look like: nr_tables = 1 table0_name =
 * net_http_request table0_url = http://httpbin.org/user-agent table0_method = GET table0_headers =
 * User-Agent:MyDAS,Accept:application/json
 */
class DASHttpOptions(options: Map[String, String]) {

  // Number of “tables” (endpoints) to define
  val nrTables: Int =
    Try(options("nr_tables").toInt).getOrElse(1)

  /**
   * Build a list of HttpTableConfig from user’s config. E.g. table0_name, table0_url, table0_method, table0_headers,
   * etc.
   */
  val tableConfigs: Seq[HttpTableConfig] = {
    (0 until nrTables).map { i =>
      val prefix = s"table${i}_"
      val tableName = options.getOrElse(prefix + "name", s"net_http_request_$i")
      val url = options.getOrElse(prefix + "url", "http://httpbin.org/get")
      val method = options.getOrElse(prefix + "method", "GET")

      // Headers might be specified as a comma-separated list of "Key:Value"
      // e.g.: "User-Agent:SteampipeTest,Accept:application/json"
      val rawHeaders = options.getOrElse(prefix + "headers", "")
      val parsedHeaders: Map[String, String] =
        rawHeaders
          .split(",")
          .map(_.trim)
          .filter(_.nonEmpty)
          .map { pair =>
            // each pair should be "Key:Value"
            val parts = pair.split(":", 2).map(_.trim)
            if (parts.length == 2) parts(0) -> parts(1)
            else "UnknownHeader" -> pair // fallback if the header line is malformed
          }
          .toMap

      HttpTableConfig(name = tableName, url = url, method = method, headers = parsedHeaders)
    }
  }
}
