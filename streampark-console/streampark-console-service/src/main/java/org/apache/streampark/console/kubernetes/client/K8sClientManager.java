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

package org.apache.streampark.console.kubernetes.client;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/** Caches fabric8 KubernetesClient instances per registered cluster id. */
@Component
public class K8sClientManager {

    private static final int CONNECT_TIMEOUT = 10_000;
    private static final int REQUEST_TIMEOUT = 20_000;

    /**
     * Short request timeout for metrics.k8s.io calls: when metrics-server is missing the aggregated
     * API hangs, and a full-length timeout would stall every monitor response on it.
     */
    private static final int METRICS_REQUEST_TIMEOUT = 5_000;

    private final ConcurrentHashMap<Long, KubernetesClient> clients = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, KubernetesClient> metricsClients = new ConcurrentHashMap<>();

    public KubernetesClient getOrCreate(Long clusterId, String kubeconfig) {
        return clients.computeIfAbsent(clusterId, id -> buildClient(kubeconfig, REQUEST_TIMEOUT));
    }

    public KubernetesClient getOrCreateMetrics(Long clusterId, String kubeconfig) {
        return metricsClients.computeIfAbsent(clusterId, id -> buildClient(kubeconfig, METRICS_REQUEST_TIMEOUT));
    }

    public void invalidate(Long clusterId) {
        closeQuietly(clients.remove(clusterId));
        closeQuietly(metricsClients.remove(clusterId));
    }

    /** Builds a throwaway client, used by connection test (no caching). */
    public KubernetesClient buildClient(String kubeconfig) {
        return buildClient(kubeconfig, REQUEST_TIMEOUT);
    }

    private KubernetesClient buildClient(String kubeconfig, int requestTimeout) {
        Config config = Config.fromKubeconfig(kubeconfig);
        config.setConnectionTimeout(CONNECT_TIMEOUT);
        config.setRequestTimeout(requestTimeout);
        return new KubernetesClientBuilder().withConfig(config).build();
    }

    private void closeQuietly(KubernetesClient client) {
        if (client != null) {
            client.close();
        }
    }
}
