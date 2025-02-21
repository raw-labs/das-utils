package com.rawlabs.das.http

import com.rawlabs.das.sdk.scala.{DASSdk, DASSdkBuilder}
import com.rawlabs.das.sdk.DASSettings

/**
 * Builder for the "http" DAS type.
 * The engine calls build() with the user-provided config, returning a new DASHttp instance.
 */
class DASHttpBuilder extends DASSdkBuilder {

  // This must match your "type" field in the config for the plugin
  override def dasType: String = "http"

  override def build(options: Map[String, String])(implicit settings: DASSettings): DASSdk = {
    new DASHttp(options)
  }
}