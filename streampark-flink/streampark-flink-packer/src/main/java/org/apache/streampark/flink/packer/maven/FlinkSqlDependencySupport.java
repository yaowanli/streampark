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

package org.apache.streampark.flink.packer.maven;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Runtime dependencies carried by the packaged SQL/CDC client jar. */
public final class FlinkSqlDependencySupport {

    private static final Artifact SNAKEYAML = new Artifact("org.yaml", "snakeyaml", "2.0");

    private static final String SNAKEYAML_GROUP = "org.yaml";
    private static final String SNAKEYAML_ARTIFACT = "snakeyaml";

    /** Coordinates are constants, so the resolved paths are stable for the whole JVM. */
    private static Set<String> cachedRuntimeJars;

    private FlinkSqlDependencySupport() {
    }

    /**
     * Merges the runtime dependencies into the job dependencies. A snakeyaml version pinned by
     * the user is dropped, not kept alongside: both jars would enter the shaded fat-jar and the
     * duplicate-class tie-break can let the user's older version win over the 2.0 the client
     * is compiled against.
     */
    public static DependencyInfo withRuntimeDependencies(DependencyInfo dependencies) {
        Set<Artifact> withoutSnakeYaml = dependencies.mavenArts().stream()
            .filter(art -> !SNAKEYAML_ARTIFACT.equals(art.artifactId())
                || !SNAKEYAML_GROUP.equals(art.groupId()))
            .collect(Collectors.toSet());
        return new DependencyInfo(withoutSnakeYaml, dependencies.extJarLibs())
            .merge(Collections.singletonList(SNAKEYAML), null);
    }

    public static synchronized Set<String> resolveRuntimeJars() throws Exception {
        if (cachedRuntimeJars == null) {
            List<File> jars = MavenTool.resolveArtifacts(Collections.singleton(SNAKEYAML));
            cachedRuntimeJars =
                jars.stream().map(File::getAbsolutePath).collect(Collectors.toSet());
        }
        return cachedRuntimeJars;
    }
}
