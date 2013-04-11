/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class EmbeddedElasticsearchServer {
  private static final String DEFAULT_DATA_DIRECTORY = "build/elasticsearch-data";

  private final Node node;
  private final String dataDirectory;

  public EmbeddedElasticsearchServer() {
    this(DEFAULT_DATA_DIRECTORY, false, false);
  }

  public EmbeddedElasticsearchServer(String dataDirectory, boolean isLocal, boolean isClientOnly) {
    this.dataDirectory = dataDirectory;

    ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder().put("path.data",
        dataDirectory);

    node = nodeBuilder().local(isLocal).client(isClientOnly).settings(elasticsearchSettings.build()).node();
  }

  public Client getClient() {
    return node.client();
  }

  public void shutdown() {
    node.close();
    deleteDataDirectory();
  }

  private void deleteDataDirectory() {
    try {
      FileUtils.deleteDirectory(new File(dataDirectory));
    } catch (IOException e) {
      throw new RuntimeException("Could not delete data directory of embedded elasticsearch server", e);
    }
  }

  public void refresIndex(String indexName) {
    new RefreshRequestBuilder(getClient().admin().indices()).setIndices(indexName).execute().actionGet();
  }

  public long countIndex(String indexName, String typeName) {
    return getClient().prepareCount(indexName).setTypes(typeName).execute().actionGet().count();
  }

  public Iterator<SearchHit> searchIndex(String indexName, String typeName) {
    return getClient().prepareSearch(indexName).setTypes(typeName).execute().actionGet().getHits().iterator();
  }
}
