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

package org.apache.streampark.console.kubernetes.assembler;

import org.apache.streampark.console.core.assembler.DtoAssembler;
import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.request.K8sClusterCreateRequest;
import org.apache.streampark.console.kubernetes.request.K8sClusterUpdateRequest;
import org.apache.streampark.console.kubernetes.response.K8sClusterResponse;

import org.springframework.beans.BeanUtils;

import java.util.List;
import java.util.stream.Collectors;

/** Converts between kubernetes cluster entities and API request/response contracts. */
public final class K8sClusterAssembler {

    private K8sClusterAssembler() {
    }

    public static K8sCluster toEntity(K8sClusterCreateRequest request) {
        if (request == null) {
            return null;
        }
        K8sCluster cluster = new K8sCluster();
        BeanUtils.copyProperties(request, cluster);
        return cluster;
    }

    public static K8sCluster toEntity(K8sClusterUpdateRequest request) {
        if (request == null) {
            return null;
        }
        K8sCluster cluster = new K8sCluster();
        BeanUtils.copyProperties(request, cluster);
        return cluster;
    }

    public static K8sClusterResponse toResponse(K8sCluster cluster) {
        return DtoAssembler.toDto(cluster, K8sClusterResponse.class);
    }

    public static List<K8sClusterResponse> toListResponse(List<K8sCluster> clusters) {
        return clusters.stream().map(K8sClusterAssembler::toResponse).collect(Collectors.toList());
    }
}
