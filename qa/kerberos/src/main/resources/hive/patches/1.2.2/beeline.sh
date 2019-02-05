# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Notice:
# Modified for keytab login

# Need arguments [host [port [db]]]
THISSERVICE=beeline
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

beeline () {
  # Change the class to SecureBeeline wrapper
  # CLASS=org.apache.hive.beeline.BeeLine;
  CLASS=org.elasticsearch.hadoop.qa.kerberos.hive.SecureBeeline;
  BEELINECLASS=org.apache.hive.beeline.BeeLine;

  # add our test jar path for ES-Hadoop
  testJarPath=`ls ${TEST_LIB}`

  # include only the beeline client jar and its dependencies
  beelineJarPath=`ls ${HIVE_LIB}/hive-beeline-*.jar`
  superCsvJarPath=`ls ${HIVE_LIB}/super-csv-*.jar`
  jlineJarPath=`ls ${HIVE_LIB}/jline-*.jar`
  jdbcStandaloneJarPath=`ls ${HIVE_LIB}/hive-jdbc-*-standalone.jar`
  hadoopClasspath=""
  if [[ -n "${HADOOP_CLASSPATH}" ]]
  then
    hadoopClasspath="${HADOOP_CLASSPATH}:"
  fi
  # Change the classpath to include our test jar
  # export HADOOP_CLASSPATH="${hadoopClasspath}${HIVE_CONF_DIR}:${beelineJarPath}:${superCsvJarPath}:${jlineJarPath}:${jdbcStandaloneJarPath}"
  export HADOOP_CLASSPATH="${hadoopClasspath}${HIVE_CONF_DIR}:${testJarPath}:${beelineJarPath}:${superCsvJarPath}:${jlineJarPath}:${jdbcStandaloneJarPath}"
  export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dlog4j.configuration=beeline-log4j.properties "

  # Change the execution to run our class which will in turn run the Beeline class
  # exec $HADOOP jar ${beelineJarPath} $CLASS $HIVE_OPTS "$@"
  exec $HADOOP jar ${testJarPath} $CLASS $BEELINECLASS $HIVE_OPTS "$@"
}

beeline_help () {
  beeline "--help"
}