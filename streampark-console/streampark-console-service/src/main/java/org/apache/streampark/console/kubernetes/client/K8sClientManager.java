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

    private final ConcurrentHashMap<Long, KubernetesClient> clients = new ConcurrentHashMap<>();

    public KubernetesClient getOrCreate(Long clusterId, String kubeconfig) {
        return clients.computeIfAbsent(clusterId, id -> buildClient(kubeconfig));
    }

    public void invalidate(Long clusterId) {
        KubernetesClient client = clients.remove(clusterId);
        if (client != null) {
            client.close();
        }
    }

    /** Builds a throwaway client, used by connection test (no caching). */
    public KubernetesClient buildClient(String kubeconfig) {
        Config config = Config.fromKubeconfig(kubeconfig);
        config.setConnectionTimeout(CONNECT_TIMEOUT);
        config.setRequestTimeout(REQUEST_TIMEOUT);
        return new KubernetesClientBuilder().withConfig(config).build();
    }
}
