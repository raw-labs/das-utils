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

package com.rawlabs.das.utils

import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.sdk.scala.{DASSdk, DASSdkBuilder}

/**
 * Builder for the "utils" DAS type. The engine calls build() with the user-provided config, returning a new DASHttp
 * instance.
 */
class DASUtilsBuilder extends DASSdkBuilder {

  // This must match your "type" field in the config for the plugin
  override def dasType: String = "utils"

  override def build(options: Map[String, String])(implicit settings: DASSettings): DASSdk = {
    new DASUtils()
  }
}
