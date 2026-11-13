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

import org.apache.streampark.console.kubernetes.client.K8sClientManager;
import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.response.ClusterOverviewResponse;
import org.apache.streampark.console.kubernetes.response.NodeResponse;
import org.apache.streampark.console.kubernetes.service.impl.K8sMonitorServiceImpl;

import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.kubernetes.api.model.NodeListBuilder;
import io.fabric8.kubernetes.api.model.NodeStatusBuilder;
import io.fabric8.kubernetes.api.model.NodeSystemInfoBuilder;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class K8sMonitorServiceTest {

    private final KubernetesServer server = new KubernetesServer(false, false);

    private K8sMonitorServiceImpl monitorService;
    private K8sCluster cluster;

    @BeforeEach
    void setUp() {
        server.before();
        K8sClientManager mockManager = new K8sClientManager() {

            @Override
            public io.fabric8.kubernetes.client.KubernetesClient buildClient(String kubeconfig) {
                return server.getClient();
            }
        };
        monitorService = new K8sMonitorServiceImpl();
        monitorService.setK8sClientManager(mockManager);
        cluster = new K8sCluster();
        cluster.setId(1L);
        cluster.setKubeconfig("fake");
    }

    @AfterEach
    void tearDown() {
        server.after();
    }

    private void mockNodes() {
        server.expect().withPath("/api/v1/nodes").andReturn(200,
            new NodeListBuilder().withItems(
                new NodeBuilder()
                    .withNewMetadata().withName("node-1").endMetadata()
                    .withStatus(new NodeStatusBuilder()
                        .withNodeInfo(new NodeSystemInfoBuilder().withKubeletVersion("v1.29.0").build())
                        .addNewCondition().withType("Ready").withStatus("True").endCondition()
                        .addNewAddress().withType("InternalIP").withAddress("10.0.0.1").endAddress()
                        .build())
                    .build(),
                new NodeBuilder()
                    .withNewMetadata().withName("node-2").endMetadata()
                    .withStatus(new NodeStatusBuilder()
                        .addNewCondition().withType("Ready").withStatus("False").endCondition()
                        .build())
                    .build())
                .build())
            .always();
    }

    private void mockPods() {
        server.expect().withPath("/api/v1/pods").andReturn(200,
            new PodListBuilder().withItems(
                new PodBuilder()
                    .withNewMetadata().withName("flink-app-abc-0").withNamespace("default").endMetadata()
                    .withNewStatus().withPhase("Running").endStatus()
                    .build(),
                new PodBuilder()
                    .withNewMetadata().withName("web-xyz").withNamespace("kube-system").endMetadata()
                    .withNewStatus().withPhase("Pending").endStatus()
                    .build())
                .build())
            .always();
    }

    private void mockNamespaces() {
        server.expect().withPath("/api/v1/namespaces").andReturn(200,
            new io.fabric8.kubernetes.api.model.NamespaceListBuilder()
                .addNewItem().withNewMetadata().withName("default").endMetadata().endItem()
                .build())
            .always();
    }

    @Test
    void testOverviewDegradesWithoutMetricsServer() {
        mockNodes();
        mockPods();
        mockNamespaces();

        ClusterOverviewResponse overview = monitorService.overview(cluster);
        assertThat(overview.getNodesTotal()).isEqualTo(2L);
        assertThat(overview.getNodesReady()).isEqualTo(1L);
        assertThat(overview.getPodsRunning()).isEqualTo(1L);
        assertThat(overview.getPodsPending()).isEqualTo(1L);
        // metrics api not mocked -> degrade path
        assertThat(overview.getMetricsServerAvailable()).isFalse();
        assertThat(overview.getCpuUsedMillis()).isEqualTo(-1L);
    }

    @Test
    void testNodes() {
        mockNodes();
        java.util.List<NodeResponse> nodes = monitorService.nodes(cluster);
        assertThat(nodes).hasSize(2);
        NodeResponse ready = nodes.get(0);
        assertThat(ready.getName()).isEqualTo("node-1");
        assertThat(ready.getStatus()).isEqualTo("Ready");
        assertThat(ready.getVersion()).isEqualTo("v1.29.0");
        assertThat(ready.getInternalIp()).isEqualTo("10.0.0.1");
        assertThat(nodes.get(1).getStatus()).isEqualTo("NotReady");
    }

    @Test
    void testPods() {
        mockPods();
        java.util.List<org.apache.streampark.console.kubernetes.response.PodResponse> pods =
            monitorService.pods(cluster, null, null);
        assertThat(pods).hasSize(2);
        assertThat(pods.get(0).getPhase()).isEqualTo("Running");
    }

    @Test
    void testNamespaces() {
        server.expect().withPath("/api/v1/namespaces").andReturn(200,
            new io.fabric8.kubernetes.api.model.NamespaceListBuilder()
                .addNewItem().withNewMetadata().withName("default").endMetadata().endItem()
                .build())
            .always();
        java.util.List<String> namespaces = monitorService.namespaces(cluster);
        assertThat(namespaces).containsExactly("default");
    }
}
