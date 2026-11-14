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

import org.apache.streampark.common.configuration.CommandLineParser;
import org.apache.streampark.common.configuration.Configuration;
import org.apache.streampark.common.configuration.ConfigurationFormat;
import org.apache.streampark.common.configuration.ConfigurationParser;
import org.apache.streampark.common.configuration.FlinkOptions;
import org.apache.streampark.common.configuration.option.ApplicationOptions;
import org.apache.streampark.common.util.DeflaterUtils;

import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.composer.PipelineExecution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** Flink CDC YAML pipeline CLI entry point. */
public final class CDCClient {

    private static final Logger LOG = LoggerFactory.getLogger(CDCClient.class);

    private static final String YAML_PREFIX = "yaml://";

    private CDCClient() {
    }

    public static void main(String[] args) throws Exception {
        Configuration parameters = CommandLineParser.parse(args);

        String cdcYaml = resolveCdcYaml(parameters);
        org.apache.flink.cdc.common.configuration.Configuration flinkConfig =
            resolveFlinkConfiguration(parameters);

        LOG.debug("Flink cdc config: {}", flinkConfig);
        LOG.debug("Flink cdc yaml: {}", cdcYaml);

        PipelineExecution.ExecutionInfo result = CDCExecutor.run(cdcYaml, flinkConfig);
        printExecutionInfo(result);
    }

    private static String resolveCdcYaml(Configuration parameters) {
        String yaml = parameters.get(ApplicationOptions.SQL);
        if (yaml == null || yaml.trim().isEmpty()) {
            throw new IllegalArgumentException("Usage: flink cdc yaml cannot be null");
        }
        String cdcYaml = DeflaterUtils.unzipString(yaml);
        if (cdcYaml.trim().isEmpty()) {
            throw new IllegalArgumentException("Usage: flink cdc yaml is invalid, please check");
        }
        return cdcYaml;
    }

    private static org.apache.flink.cdc.common.configuration.Configuration resolveFlinkConfiguration(
                                                                                                     Configuration parameters) {
        Map<String, String> configMap = new HashMap<>();

        String flinkConf = parameters.get(FlinkOptions.FLINK_CONFIGURATION);
        if (flinkConf != null && !flinkConf.trim().isEmpty()) {
            configMap.putAll(parseYamlText(DeflaterUtils.unzipString(flinkConf)));
        }

        // a cdc yaml job runs fine without an extra application config, so conf is optional
        String appConf = parameters.getOptional(ApplicationOptions.CONFIG).orElse(null);
        if (appConf != null && appConf.startsWith(YAML_PREFIX)) {
            configMap.putAll(parseYamlText(DeflaterUtils.unzipString(appConf.substring(YAML_PREFIX.length()))));
        }

        // the pipeline composer reads operator parallelism from the flink-cdc option key
        // ("parallelism"), not from Flink's CoreOptions.DEFAULT_PARALLELISM
        Integer parallelism = parameters.get(FlinkOptions.PARALLELISM);
        if (parallelism != null) {
            configMap.put(PipelineOptions.PIPELINE_PARALLELISM.key(), parallelism.toString());
        }

        return org.apache.flink.cdc.common.configuration.Configuration.fromMap(configMap);
    }

    private static Map<String, String> parseYamlText(String yamlText) {
        Configuration configuration = ConfigurationParser.parse(
            yamlText, ConfigurationFormat.YAML, "inline flink cdc application configuration");
        return configuration.toMap();
    }

    private static void printExecutionInfo(PipelineExecution.ExecutionInfo info) {
        System.out.println("Pipeline has been submitted to cluster.");
        System.out.printf("Job ID: %s\n", info.getId());
        System.out.printf("Job Description: %s\n", info.getDescription());
    }
}
