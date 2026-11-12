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

package org.apache.streampark.flink.cdc.cli;

import org.apache.flink.cdc.cli.parser.PipelineDefinitionParser;
import org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.composer.PipelineComposer;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

/**
 * Composes and executes one Flink CDC YAML pipeline definition against CDC 3.2 through 3.5.
 *
 * <p>The composer entry points drifted across the release trains, so the factory is resolved at
 * runtime instead of compiling against one train's API. Under the StreamPark submission flow both
 * outcomes behave identically: launched through the Flink client's packaged program the
 * environment intercepts {@code execute()} and hands the job graph back to the submitting client,
 * while in application mode (YARN/Kubernetes) the environment submits directly against the
 * bootstrap configuration.
 */
public final class CDCExecutor {

    private CDCExecutor() {
    }

    public static PipelineExecution.ExecutionInfo run(
                                                      String pipelineString,
                                                      Configuration globalPipelineConfig) throws Exception {
        PipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(pipelineString, globalPipelineConfig);
        PipelineComposer composer = createComposer(globalPipelineConfig);
        return composer.compose(pipelineDef).execute();
    }

    private static PipelineComposer createComposer(Configuration cdcConfig) throws Exception {
        try {
            // CDC 3.4+: application-cluster composer bound to the caller's execution environment.
            Method applicationCluster = FlinkPipelineComposer.class.getMethod(
                "ofApplicationCluster", StreamExecutionEnvironment.class);
            return (PipelineComposer) applicationCluster.invoke(
                null, StreamExecutionEnvironment.getExecutionEnvironment());
        } catch (NoSuchMethodException ignored) {
            // fall through to the legacy factory used by CDC 3.2/3.3
        }
        Class<?> utils = Class.forName("org.apache.flink.cdc.cli.utils.FlinkEnvironmentUtils");
        Method createComposer = utils.getMethod(
            "createComposer",
            boolean.class,
            Configuration.class,
            List.class,
            SavepointRestoreSettings.class);
        return (PipelineComposer) createComposer.invoke(
            null, true, cdcConfig, Collections.emptyList(), SavepointRestoreSettings.none());
    }
}
