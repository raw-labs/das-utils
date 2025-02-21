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

import java.net.http.HttpClient

import com.typesafe.scalalogging.StrictLogging

/**
 * Manages the underlying HttpClient. In a more complex scenario, you could handle connection pooling, authentication
 * tokens, etc.
 */
class DASHttpConnector extends StrictLogging {

  private lazy val httpClient: HttpClient = {
    logger.info("Creating a new Java HttpClient for the HTTP DAS.")
    HttpClient.newHttpClient()
  }

  def getHttpClient: HttpClient = httpClient
}
