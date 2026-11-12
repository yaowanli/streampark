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

package org.apache.streampark.console.kubernetes.controller;

import org.apache.streampark.console.base.domain.RestResponseBody;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.response.ClusterOverviewResponse;
import org.apache.streampark.console.kubernetes.response.DeploymentResponse;
import org.apache.streampark.console.kubernetes.response.EventResponse;
import org.apache.streampark.console.kubernetes.response.FlinkAppResponse;
import org.apache.streampark.console.kubernetes.response.HistoryResponse;
import org.apache.streampark.console.kubernetes.response.NodeResponse;
import org.apache.streampark.console.kubernetes.response.PodResponse;
import org.apache.streampark.console.kubernetes.service.K8sClusterService;
import org.apache.streampark.console.kubernetes.service.K8sMonitorService;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("k8s/monitor")
public class K8sMonitorController {

    @Autowired
    private K8sClusterService k8sClusterService;

    @Autowired
    private K8sMonitorService k8sMonitorService;

    @GetMapping("overview")
    @RequiresPermissions("k8sMonitor:view")
    public RestResponseBody<ClusterOverviewResponse> overview(@RequestParam Long clusterId) {
        return RestResponseBody.success(
            k8sMonitorService.overview(cluster(clusterId)));
    }

    @GetMapping("nodes")
    @RequiresPermissions("k8sMonitor:view")
    public RestResponseBody<List<NodeResponse>> nodes(@RequestParam Long clusterId) {
        return RestResponseBody.success(
            k8sMonitorService.nodes(cluster(clusterId)));
    }

    @GetMapping("workloads")
    @RequiresPermissions("k8sMonitor:view")
    public RestResponseBody<List<DeploymentResponse>> workloads(
                                                                @RequestParam Long clusterId,
                                                                @RequestParam(required = false) String namespace,
                                                                @RequestParam(required = false) String keyword) {
        return RestResponseBody.success(
            k8sMonitorService.deployments(cluster(clusterId), namespace, keyword));
    }

    @GetMapping("pods")
    @RequiresPermissions("k8sMonitor:view")
    public RestResponseBody<List<PodResponse>> pods(
                                                    @RequestParam Long clusterId,
                                                    @RequestParam(required = false) String namespace,
                                                    @RequestParam(required = false) String keyword) {
        return RestResponseBody.success(
            k8sMonitorService.pods(cluster(clusterId), namespace, keyword));
    }

    @GetMapping("events")
    @RequiresPermissions("k8sMonitor:view")
    public RestResponseBody<List<EventResponse>> events(
                                                        @RequestParam Long clusterId,
                                                        @RequestParam(required = false) String namespace,
                                                        @RequestParam(defaultValue = "50") int limit) {
        return RestResponseBody.success(
            k8sMonitorService.events(cluster(clusterId), namespace, limit));
    }

    @GetMapping("namespaces")
    @RequiresPermissions("k8sMonitor:view")
    public RestResponseBody<List<String>> namespaces(@RequestParam Long clusterId) {
        return RestResponseBody.success(
            k8sMonitorService.namespaces(cluster(clusterId)));
    }

    @GetMapping("history")
    @RequiresPermissions("k8sMonitor:view")
    public RestResponseBody<HistoryResponse> history(
                                                     @RequestParam Long clusterId,
                                                     @RequestParam(defaultValue = "cluster") String kind,
                                                     @RequestParam(required = false) String name,
                                                     @RequestParam(required = false) String namespace,
                                                     @RequestParam(defaultValue = "6h") String range) {
        return RestResponseBody.success(
            k8sMonitorService.history(cluster(clusterId), kind, name, namespace, range));
    }

    @GetMapping("flinkapps")
    @RequiresPermissions("k8sMonitor:view")
    public RestResponseBody<List<FlinkAppResponse>> flinkApps(@RequestParam Long clusterId) {
        return RestResponseBody.success(
            k8sMonitorService.flinkApps(cluster(clusterId)));
    }

    private K8sCluster cluster(Long clusterId) {
        K8sCluster cluster = k8sClusterService.getById(clusterId);
        ApiAlertException.throwIfNull(
            cluster, "The kubernetes cluster registration doesn't exist.");
        return cluster;
    }
}
