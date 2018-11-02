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

package org.elasticsearch.hadoop.gradle.fixture.hadoop

import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.HadoopClusterConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.RoleConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.ServiceConfiguration
import org.junit.Test

import static org.junit.Assert.*

class HadoopClusterConfigurationTest {

    @Test
    void testConfigurations() {
        HadoopClusterConfiguration cluster = new HadoopClusterConfiguration(null, "test");

        cluster.service('hdfs').role('datanode').setInstances(3)

        cluster.service('hdfs').role('namenode').addSetting('test1', 'roleValue')
        cluster.service('hdfs').role('namenode').addSetting('test2', 'roleValue')

        cluster.service('hdfs').role('namenode').instance(0).addSetting('test1', 'instanceValue')

        cluster.service('hdfs') {
            addSetting('test1', 'serviceValue')
        }
        cluster.service('hdfs').addSetting('test2', 'serviceValue')
        cluster.service('hdfs').addSetting('test3', 'serviceValue')

        List<InstanceConfiguration> instances = cluster.getServices()
                .collect() { ServiceConfiguration s -> s.getRoles() }.flatten()
                .collect() { RoleConfiguration r -> r.getInstances() }.flatten()

        InstanceConfiguration namenode0 = instances[0]
        assertEquals('namenode', namenode0.getRoleConf().getRoleDescriptor().roleName())
        assertEquals(0, namenode0.getInstance())
        assertEquals('instanceValue', namenode0.getSettingsContainer().getSettings().get('test1'))

        InstanceConfiguration datanode0 = instances[1]
        assertEquals('datanode', datanode0.getRoleConf().getRoleDescriptor().roleName())
        assertEquals(0, datanode0.getInstance())
        assertEquals('serviceValue', datanode0.getSettingsContainer().getSettings().get('test1'))

        InstanceConfiguration datanode1 = instances[2]
        assertEquals('datanode', datanode1.getRoleConf().getRoleDescriptor().roleName())
        assertEquals(1, datanode1.getInstance())
        assertEquals('serviceValue', datanode1.getSettingsContainer().getSettings().get('test1'))

        InstanceConfiguration datanode2 = instances[3]
        assertEquals('datanode', datanode2.getRoleConf().getRoleDescriptor().roleName())
        assertEquals(2, datanode2.getInstance())
        assertEquals('serviceValue', datanode2.getSettingsContainer().getSettings().get('test1'))
    }
}