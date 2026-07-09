/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.spark.sql

import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class DefaultSourceTest {

  @Test
  def parameters(): Unit = {
    val settings = new mutable.LinkedHashMap[String, String]()
    settings.put("path", "wrong")
    settings.put("resource", "wrong")
    settings.put("es_resource", "preferred")
    settings.put("unrelated", "unrelated")

    val relation = new DefaultSource().params(settings.toMap)

    assertEquals(Map("es.resource" -> "preferred", "es.unrelated" -> "unrelated"), relation)
  }

  @Test
  def relationToStringMasksSensitiveSettings(): Unit = {
    val relation = ElasticsearchRelation(Map(
      ConfigurationOptions.ES_NET_HTTP_AUTH_USER -> "http-user",
      ConfigurationOptions.ES_NET_HTTP_AUTH_PASS -> "http-pass",
      ConfigurationOptions.ES_NET_PROXY_HTTP_USER -> "proxy-http-user",
      ConfigurationOptions.ES_NET_PROXY_HTTP_PASS -> "proxy-http-pass",
      ConfigurationOptions.ES_NET_PROXY_HTTPS_USER -> "proxy-https-user",
      ConfigurationOptions.ES_NET_PROXY_HTTPS_PASS -> "proxy-https-pass",
      ConfigurationOptions.ES_NET_PROXY_SOCKS_USER -> "proxy-socks-user",
      ConfigurationOptions.ES_NET_PROXY_SOCKS_PASS -> "proxy-socks-pass",
      ConfigurationOptions.ES_NET_SSL_TRUST_STORE_PASS -> "truststore-pass",
      ConfigurationOptions.ES_NET_SSL_KEYSTORE_PASS -> "keystore-pass",
      "es.nodes" -> "localhost"
    ), null)

    val rendered = relation.toString

    assertTrue(rendered.contains(s"${ConfigurationOptions.ES_NET_HTTP_AUTH_USER}=****"))
    assertTrue(rendered.contains(s"${ConfigurationOptions.ES_NET_HTTP_AUTH_PASS}=****"))
    assertTrue(rendered.contains(s"${ConfigurationOptions.ES_NET_PROXY_HTTP_USER}=****"))
    assertTrue(rendered.contains(s"${ConfigurationOptions.ES_NET_PROXY_HTTP_PASS}=****"))
    assertTrue(rendered.contains(s"${ConfigurationOptions.ES_NET_PROXY_HTTPS_USER}=****"))
    assertTrue(rendered.contains(s"${ConfigurationOptions.ES_NET_PROXY_HTTPS_PASS}=****"))
    assertTrue(rendered.contains(s"${ConfigurationOptions.ES_NET_PROXY_SOCKS_USER}=****"))
    assertTrue(rendered.contains(s"${ConfigurationOptions.ES_NET_PROXY_SOCKS_PASS}=****"))
    assertTrue(rendered.contains(s"${ConfigurationOptions.ES_NET_SSL_TRUST_STORE_PASS}=****"))
    assertTrue(rendered.contains(s"${ConfigurationOptions.ES_NET_SSL_KEYSTORE_PASS}=****"))
    assertTrue(rendered.contains("es.nodes=localhost"))

    Seq(
      "http-user",
      "http-pass",
      "proxy-http-user",
      "proxy-http-pass",
      "proxy-https-user",
      "proxy-https-pass",
      "proxy-socks-user",
      "proxy-socks-pass",
      "truststore-pass",
      "keystore-pass"
    ).foreach { secret =>
      assertFalse(rendered.contains(secret))
    }
  }
}
