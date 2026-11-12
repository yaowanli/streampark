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
import org.apache.streampark.console.base.web.FormOrJson;
import org.apache.streampark.console.kubernetes.assembler.K8sClusterAssembler;
import org.apache.streampark.console.kubernetes.client.K8sClientManager;
import org.apache.streampark.console.kubernetes.request.K8sClusterCreateRequest;
import org.apache.streampark.console.kubernetes.request.K8sClusterTestRequest;
import org.apache.streampark.console.kubernetes.request.K8sClusterUpdateRequest;
import org.apache.streampark.console.kubernetes.response.K8sClusterResponse;
import org.apache.streampark.console.kubernetes.response.K8sClusterTestResponse;
import org.apache.streampark.console.kubernetes.service.K8sClusterService;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

import java.util.List;

@Slf4j
@Validated
@RestController
@RequestMapping("k8s/cluster")
public class K8sClusterController {

    @Autowired
    private K8sClusterService k8sClusterService;

    @Autowired
    private K8sClientManager k8sClientManager;

    @PostMapping("list")
    public RestResponseBody<List<K8sClusterResponse>> list() {
        return RestResponseBody.success(
            K8sClusterAssembler.toListResponse(k8sClusterService.listAll()));
    }

    @PostMapping("create")
    @RequiresPermissions("k8sCluster:create")
    public RestResponseBody<Boolean> create(@Valid @FormOrJson K8sClusterCreateRequest request) {
        return RestResponseBody.success(
            k8sClusterService.createCluster(K8sClusterAssembler.toEntity(request)));
    }

    @PostMapping("update")
    @RequiresPermissions("k8sCluster:update")
    public RestResponseBody<Void> update(@Valid @FormOrJson K8sClusterUpdateRequest request) {
        k8sClusterService.updateCluster(K8sClusterAssembler.toEntity(request));
        return RestResponseBody.success();
    }

    @PostMapping("delete")
    @RequiresPermissions("k8sCluster:delete")
    public RestResponseBody<Void> delete(@Valid @FormOrJson K8sClusterUpdateRequest request) {
        k8sClusterService.remove(K8sClusterAssembler.toEntity(request));
        return RestResponseBody.success();
    }

    @PostMapping("test")
    public RestResponseBody<K8sClusterTestResponse> test(
                                                         @Valid @FormOrJson K8sClusterTestRequest request) {
        K8sClusterTestResponse response = new K8sClusterTestResponse();
        try (KubernetesClient client = k8sClientManager.buildClient(request.getKubeconfig())) {
            response.setConnected(true);
            response.setVersion(client.getKubernetesVersion().getGitVersion());
            response.setMessage("Connection succeeded.");
        } catch (KubernetesClientException | IllegalArgumentException e) {
            log.warn("Kubernetes cluster connection test failed: {}", e.getMessage());
            response.setConnected(false);
            response.setMessage(e.getMessage());
        }
        return RestResponseBody.success(response);
    }
}
