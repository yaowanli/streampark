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

import org.apache.streampark.console.SpringUnitTestBase;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.kubernetes.entity.K8sCluster;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
class K8sClusterServiceTest extends SpringUnitTestBase {

    @Autowired
    private K8sClusterService k8sClusterService;

    @AfterEach
    void cleanTestRecordsInDatabase() {
        k8sClusterService.remove(new QueryWrapper<>());
    }

    private K8sCluster mockCluster(String name) {
        K8sCluster cluster = new K8sCluster();
        cluster.setClusterName(name);
        cluster.setKubeconfig("apiVersion: v1\nkind: Config");
        cluster.setDescription("test cluster");
        return cluster;
    }

    @Test
    void testCreateAndDuplicateNameRejected() {
        assertThat(k8sClusterService.createCluster(mockCluster("test-cluster"))).isTrue();
        assertThatThrownBy(() -> k8sClusterService.createCluster(mockCluster("test-cluster")))
            .isInstanceOf(ApiAlertException.class);
    }

    @Test
    void testUpdate() {
        K8sCluster cluster = mockCluster("c1");
        k8sClusterService.createCluster(cluster);
        K8sCluster fromDb = k8sClusterService.getById(cluster.getId());
        fromDb.setPrometheusUrl("http://prometheus:9090");
        k8sClusterService.updateCluster(fromDb);
        assertThat(k8sClusterService.getById(cluster.getId()).getPrometheusUrl())
            .isEqualTo("http://prometheus:9090");
    }

    @Test
    void testRemove() {
        K8sCluster cluster = mockCluster("c2");
        k8sClusterService.createCluster(cluster);
        k8sClusterService.remove(cluster);
        assertThat(k8sClusterService.getById(cluster.getId())).isNull();
    }
}
