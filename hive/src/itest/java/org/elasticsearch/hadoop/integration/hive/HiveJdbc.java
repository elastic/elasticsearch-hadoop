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
package org.elasticsearch.hadoop.integration.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class HiveJdbc implements HiveInstance {

    private static final String HIVE = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static final String HIVE2 = "org.apache.hive.jdbc.HiveDriver";

    private String url;
    private Connection connection;

    public HiveJdbc(String url) {
        this.url = url;
        String driverName = (url.contains("hive2") ? HIVE2 : HIVE);
        // ugly but this is what Hive uses internally anyway
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void start() throws Exception {
        connection = DriverManager.getConnection(url);
    }

    @Override
    public List<String> execute(String statement) throws Exception {
        Statement stmt = connection.createStatement();
        List<String> results = new ArrayList<String>();
        try {
            if (stmt.execute(statement)) {
                ResultSet rs = stmt.getResultSet();
                try {
                    ResultSetMetaData md = rs.getMetaData();

                    while (rs.next()) {
                        for (int i = 0; i < md.getColumnCount(); i++) {
                            results.add(rs.getString(i + 1));
                        }
                    }
                } finally {
                    rs.close();
                }
            }
        } finally {
            stmt.close();
        }

        return results;
    }

    @Override
    public void stop() throws Exception {
        connection.close();
    }
}