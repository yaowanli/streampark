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
import org.apache.streampark.console.core.bean.MavenPom;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects the Flink CDC connector version declared by an application dependency.
 *
 * <p>The version pins the CDC framework release train the job expects at runtime, so the detected
 * minor version drives which bundled CDC client jar is selected for the job. Two artifact families
 * are recognized: the yaml pipeline connectors ({@code flink-cdc-pipeline-connector-mysql} and
 * friends, the ones a CDC yaml job actually runs with) and the SQL/DataStream source connectors
 * ({@code flink-connector-mysql-cdc}, {@code flink-sql-connector-mysql-cdc}). Connector 1.x/2.x
 * releases are ignored: they predate the yaml pipeline API and only serve Flink SQL jobs.
 */
public final class FlinkCdcUtils {

    private static final Pattern CDC_COORDINATE =
        Pattern.compile("flink-(sql-)?connector-.+-cdc|flink-cdc-pipeline-connector-.+");

    private static final Pattern CDC_JAR_FILE = Pattern.compile(
        "flink-(sql-)?connector-.+-cdc-(\\d+\\.\\d+\\.\\d+).*\\.jar"
            + "|flink-cdc-pipeline-connector-.+-(\\d+\\.\\d+\\.\\d+).*\\.jar");

    private static final Pattern CDC_MINOR_VERSION = Pattern.compile("^3\\.(\\d+)");

    private FlinkCdcUtils() {
    }

    /**
     * Returns the highest CDC 3.x minor version (for example {@code 3} for 3.3.0) declared by the
     * application's pipeline connectors, or empty when no 3.x CDC connector is present.
     */
    public static Optional<Integer> detectConnectorMinorVersion(Dependency dependency) {
        if (dependency == null) {
            return Optional.empty();
        }
        Optional<Integer> fromPom = dependency.getPom().stream()
            .filter(FlinkCdcUtils::isCdcCoordinate)
            .map(MavenPom::getVersion)
            .map(FlinkCdcUtils::extractMinor)
            .flatMap(Optional::stream)
            .reduce(Integer::max);

        return fromPom.isPresent()
            ? fromPom
            : dependency.getJar().stream()
                .map(FlinkCdcUtils::extractMinorFromJarFile)
                .flatMap(Optional::stream)
                .reduce(Integer::max);
    }

    private static boolean isCdcCoordinate(MavenPom pom) {
        return pom.getVersion() != null
            && CDC_COORDINATE.matcher(pom.getArtifactId()).matches();
    }

    private static Optional<Integer> extractMinor(String version) {
        if (version == null) {
            return Optional.empty();
        }
        Matcher matcher = CDC_MINOR_VERSION.matcher(version);
        return matcher.find() ? Optional.of(Integer.valueOf(matcher.group(1))) : Optional.empty();
    }

    private static Optional<Integer> extractMinorFromJarFile(String jarName) {
        if (jarName == null) {
            return Optional.empty();
        }
        Matcher matcher = CDC_JAR_FILE.matcher(jarName);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        return extractMinor(matcher.group(2) != null ? matcher.group(2) : matcher.group(3));
    }
}
