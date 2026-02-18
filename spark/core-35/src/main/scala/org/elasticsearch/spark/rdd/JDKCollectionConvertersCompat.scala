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
package org.elasticsearch.spark.rdd

/**
 * Magic to get cross-compiling access to scala.jdk.CollectionConverter
 * with a fallback on scala.collection.JavaConverters, without deprecation
 * warning in any Scala version.
 * From https://github.com/scala/scala-collection-compat/issues/208#issuecomment-497735669
 */
private[elasticsearch] object JDKCollectionConvertersCompat {
  object Scope1 {
    object jdk {
      type CollectionConverters = Int
    }
  }
  import Scope1._

  object Scope2 {
    import scala.collection.{JavaConverters => CollectionConverters}
    object Inner {
      import scala._
      import jdk.CollectionConverters
      val Converters = CollectionConverters
    }
  }

  val Converters = Scope2.Inner.Converters
}