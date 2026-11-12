/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.console.core.util;

import org.apache.streampark.console.core.bean.Dependency;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the CDC connector version detection from application dependencies. */
class FlinkCdcUtilsTest {

    @Test
    void emptyDependencyHasNoVersion() {
        assertThat(FlinkCdcUtils.detectConnectorMinorVersion(null)).isEmpty();
        assertThat(FlinkCdcUtils.detectConnectorMinorVersion(new Dependency())).isEmpty();
    }

    @Test
    void detectFromMavenCoordinate() {
        Dependency dependency = Dependency.toDependency(
            "{\"pom\":[{\"groupId\":\"org.apache.flink\","
                + "\"artifactId\":\"flink-connector-mysql-cdc\",\"version\":\"3.3.0\"}]}");
        assertThat(FlinkCdcUtils.detectConnectorMinorVersion(dependency)).contains(3);
    }

    @Test
    void detectFromSqlConnectorCoordinate() {
        Dependency dependency = Dependency.toDependency(
            "{\"pom\":[{\"groupId\":\"org.apache.flink\","
                + "\"artifactId\":\"flink-sql-connector-mysql-cdc\",\"version\":\"3.4.0\"}]}");
        assertThat(FlinkCdcUtils.detectConnectorMinorVersion(dependency)).contains(4);
    }

    @Test
    void legacyTwoXConnectorIsIgnored() {
        Dependency dependency = Dependency.toDependency(
            "{\"pom\":[{\"groupId\":\"com.ververica\","
                + "\"artifactId\":\"flink-connector-mysql-cdc\",\"version\":\"2.4.1\"}]}");
        assertThat(FlinkCdcUtils.detectConnectorMinorVersion(dependency)).isEmpty();
    }

    @Test
    void detectFromUploadedJarFile() {
        Dependency dependency = Dependency.toDependency(
            "{\"jar\":[\"flink-sql-connector-mysql-cdc-3.2.1.jar\"]}");
        assertThat(FlinkCdcUtils.detectConnectorMinorVersion(dependency)).contains(2);
    }

    @Test
    void highestVersionWins() {
        Dependency dependency = Dependency.toDependency(
            "{\"pom\":["
                + "{\"groupId\":\"org.apache.flink\","
                + "\"artifactId\":\"flink-connector-mysql-cdc\",\"version\":\"3.2.1\"},"
                + "{\"groupId\":\"org.apache.flink\","
                + "\"artifactId\":\"flink-connector-starrocks-cdc\",\"version\":\"3.5.0\"}],"
                + "\"jar\":[\"flink-connector-postgres-cdc-3.3.0.jar\"]}");
        assertThat(FlinkCdcUtils.detectConnectorMinorVersion(dependency)).contains(5);
    }

    @Test
    void nonCdcArtifactsAreIgnored() {
        Dependency dependency = Dependency.toDependency(
            "{\"pom\":[{\"groupId\":\"org.apache.flink\","
                + "\"artifactId\":\"flink-connector-jdbc\",\"version\":\"3.3.0-1.20\"}],"
                + "\"jar\":[\"mysql-connector-j-8.0.33.jar\"]}");
        assertThat(FlinkCdcUtils.detectConnectorMinorVersion(dependency)).isEmpty();
        Assertions.assertTrue(dependency.getPom().size() == 1);
    }
}
