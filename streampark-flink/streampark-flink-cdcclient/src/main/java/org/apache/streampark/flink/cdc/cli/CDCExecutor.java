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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Composes and executes one Flink CDC YAML pipeline definition.
 *
 * <p>The application-cluster composer lets this entry point work in every StreamPark submission
 * path: launched through the Flink client's packaged program the environment intercepts {@code
 * execute()} and hands the job graph back to the submitting client, while in application mode
 * (YARN/Kubernetes) the environment submits directly against the bootstrap configuration.
 */
public final class CDCExecutor {

    private CDCExecutor() {
    }

    public static PipelineExecution.ExecutionInfo run(
                                                      String pipelineString,
                                                      Configuration globalPipelineConfig) throws Exception {
        PipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(pipelineString, globalPipelineConfig);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        PipelineComposer composer = FlinkPipelineComposer.ofApplicationCluster(env);
        return composer.compose(pipelineDef).execute();
    }
}
