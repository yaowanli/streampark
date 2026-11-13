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

import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.response.ClusterOverviewResponse;
import org.apache.streampark.console.kubernetes.response.DeploymentResponse;
import org.apache.streampark.console.kubernetes.response.EventResponse;
import org.apache.streampark.console.kubernetes.response.FlinkAppResponse;
import org.apache.streampark.console.kubernetes.response.HistoryResponse;
import org.apache.streampark.console.kubernetes.response.NodeResponse;
import org.apache.streampark.console.kubernetes.response.PodResponse;

import java.util.List;

public interface K8sMonitorService {

    ClusterOverviewResponse overview(K8sCluster cluster);

    List<NodeResponse> nodes(K8sCluster cluster);

    List<DeploymentResponse> deployments(K8sCluster cluster, String namespace, String keyword);

    List<PodResponse> pods(K8sCluster cluster, String namespace, String keyword);

    List<EventResponse> events(K8sCluster cluster, String namespace, int limit);

    List<String> namespaces(K8sCluster cluster);

    /** @param kind cluster|node|pod */
    HistoryResponse history(K8sCluster cluster, String kind, String name, String namespace, String range);

    List<FlinkAppResponse> flinkApps(K8sCluster cluster);
}
