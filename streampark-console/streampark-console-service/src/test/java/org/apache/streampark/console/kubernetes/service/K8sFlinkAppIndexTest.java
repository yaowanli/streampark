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

package org.apache.streampark.console.kubernetes.service;

import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.kubernetes.service.impl.K8sMonitorServiceImpl;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class K8sFlinkAppIndexTest {

    private FlinkApplication app(long id, String ns, String jobName, int deployMode, Long flinkClusterId) {
        FlinkApplication app = new FlinkApplication();
        app.setId(id);
        app.setK8sNamespace(ns);
        app.setJobName(jobName);
        app.setDeployMode(deployMode);
        app.setFlinkClusterId(flinkClusterId);
        return app;
    }

    private FlinkCluster flinkCluster(long id, String ns, String clusterId) {
        FlinkCluster cluster = new FlinkCluster();
        cluster.setId(id);
        cluster.setK8sNamespace(ns);
        cluster.setClusterId(clusterId);
        return cluster;
    }

    @Test
    void testDeploymentKeyOfApplicationModeUsesJobName() {
        Map<Long, FlinkCluster> clusters = new HashMap<>();
        FlinkApplication app = app(1L, "default", "app-one",
            FlinkDeployMode.KUBERNETES_NATIVE_APPLICATION.getMode(), null);
        assertThat(K8sMonitorServiceImpl.deploymentKeyOf(app, clusters)).isEqualTo("default/app-one");
        assertThat(K8sMonitorServiceImpl.deploymentKeyOf(
            app(2L, "default", "", FlinkDeployMode.KUBERNETES_NATIVE_APPLICATION.getMode(), null),
            clusters)).isNull();
    }

    @Test
    void testDeploymentKeyOfSessionModeUsesFlinkCluster() {
        Map<Long, FlinkCluster> clusters = new HashMap<>();
        clusters.put(10L, flinkCluster(10L, "flink-ns", "session-cluster-1"));

        FlinkApplication sessionApp = app(
            3L, "default", "job-of-session", FlinkDeployMode.KUBERNETES_NATIVE_SESSION.getMode(), 10L);
        assertThat(K8sMonitorServiceImpl.deploymentKeyOf(sessionApp, clusters))
            .isEqualTo("flink-ns/session-cluster-1");

        // referenced flink cluster missing -> no deployment identity
        assertThat(K8sMonitorServiceImpl.deploymentKeyOf(
            app(4L, "default", "x", FlinkDeployMode.KUBERNETES_NATIVE_SESSION.getMode(), 99L),
            clusters)).isNull();
        assertThat(K8sMonitorServiceImpl.deploymentKeyOf(
            app(5L, "default", "x", FlinkDeployMode.KUBERNETES_NATIVE_SESSION.getMode(), null),
            clusters)).isNull();
    }
}
