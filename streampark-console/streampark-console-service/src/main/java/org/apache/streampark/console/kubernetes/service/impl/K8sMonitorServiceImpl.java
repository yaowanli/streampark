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

package org.apache.streampark.console.kubernetes.service.impl;

import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.mapper.FlinkApplicationMapper;
import org.apache.streampark.console.core.service.FlinkClusterService;
import org.apache.streampark.console.kubernetes.client.K8sClientManager;
import org.apache.streampark.console.kubernetes.client.K8sPrometheusClient;
import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.response.ClusterOverviewResponse;
import org.apache.streampark.console.kubernetes.response.DeploymentResponse;
import org.apache.streampark.console.kubernetes.response.EventResponse;
import org.apache.streampark.console.kubernetes.response.FlinkAppResponse;
import org.apache.streampark.console.kubernetes.response.HistoryResponse;
import org.apache.streampark.console.kubernetes.response.NodeResponse;
import org.apache.streampark.console.kubernetes.response.PodResponse;
import org.apache.streampark.console.kubernetes.service.K8sMonitorService;
import org.apache.streampark.console.kubernetes.util.K8sQuantityUtils;
import org.apache.streampark.flink.kubernetes.CacheKey;
import org.apache.streampark.flink.kubernetes.FlinkKubernetesWatcher;
import org.apache.streampark.flink.kubernetes.model.JobStatusCV;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.google.common.annotations.VisibleForTesting;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.metrics.v1beta1.NodeMetrics;
import io.fabric8.kubernetes.api.model.metrics.v1beta1.PodMetrics;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Service
public class K8sMonitorServiceImpl implements K8sMonitorService {

    private static final String LABEL_NODE_ROLE_PREFIX = "node-role.kubernetes.io/";

    /** kubernetes-safe name charset, also keeps PromQL label matchers injection-free. */
    private static final Pattern K8S_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]+$");

    private static final Map<String, Long> RANGE_SECONDS = Map.of(
        "1h", 3600L,
        "6h", 6 * 3600L,
        "24h", 24 * 3600L,
        "7d", 7 * 86400L);

    /** Hard wall-clock cap for a metrics.k8s.io call: the apiserver may hold a 503 for ~20s when
     * metrics-server is down, and fabric8's socket timeouts cannot cut that short. */
    private static final long METRICS_TIMEOUT_MS = 5_000;

    /** After a metrics failure, skip further metrics calls for this long instead of paying the timeout each refresh. */
    private static final long METRICS_BACKOFF_MS = 60_000;

    private final ConcurrentMap<Long, Long> metricsBackoffUntil = new ConcurrentHashMap<>();

    private final ExecutorService metricsExecutor = Executors.newFixedThreadPool(2, r -> {
        Thread t = new Thread(r, "k8s-metrics");
        t.setDaemon(true);
        return t;
    });

    @Autowired
    private K8sClientManager k8sClientManager;

    @Autowired
    private FlinkApplicationMapper flinkApplicationMapper;

    @Autowired
    private FlinkClusterService flinkClusterService;

    @Autowired
    private FlinkKubernetesWatcher flinkK8sWatcher;

    @VisibleForTesting
    public void setK8sClientManager(K8sClientManager k8sClientManager) {
        this.k8sClientManager = k8sClientManager;
    }

    /**
     * Runs a metrics.k8s.io call under a hard wall-clock cap. When metrics-server is missing the
     * apiserver may hold the request for ~20s before answering 503, which no client socket timeout
     * reliably cuts short, so the call runs on a worker thread bounded by future.get(). Failures
     * arm a per-cluster backoff so subsequent refreshes skip the call entirely. Returns null when
     * metrics are unavailable.
     */
    private <T> T metricsCall(K8sCluster cluster, Function<KubernetesClient, T> call) {
        Long clusterId = cluster.getId();
        Long until = metricsBackoffUntil.get(clusterId);
        if (until != null && until > System.currentTimeMillis()) {
            return null;
        }
        KubernetesClient metricsClient =
            k8sClientManager.getOrCreateMetrics(clusterId, cluster.getKubeconfig());
        Future<T> future = metricsExecutor.submit(() -> call.apply(metricsClient));
        try {
            return future.get(METRICS_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            log.warn("metrics.k8s.io call timed out after {}ms for cluster {}, backing off for {}ms",
                METRICS_TIMEOUT_MS, clusterId, METRICS_BACKOFF_MS);
            metricsBackoffUntil.put(clusterId, System.currentTimeMillis() + METRICS_BACKOFF_MS);
            return null;
        } catch (ExecutionException e) {
            log.warn("metrics.k8s.io call failed for cluster {}, backing off for {}ms: {}",
                clusterId, METRICS_BACKOFF_MS, e.getCause().getMessage());
            metricsBackoffUntil.put(clusterId, System.currentTimeMillis() + METRICS_BACKOFF_MS);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            return null;
        }
    }

    // ---------- overview ----------
    @Override
    public ClusterOverviewResponse overview(K8sCluster cluster) {
        KubernetesClient client = k8sClientManager.getOrCreate(cluster.getId(), cluster.getKubeconfig());
        List<Node> nodes = client.nodes().list().getItems();
        List<Pod> pods = client.pods().inAnyNamespace().list().getItems();
        ClusterOverviewResponse resp = new ClusterOverviewResponse();
        resp.setNodesTotal((long) nodes.size());
        resp.setNodesReady(nodes.stream().filter(this::isNodeReady).count());
        resp.setPodsRunning(
            pods.stream().filter(p -> "Running".equals(p.getStatus().getPhase())).count());
        resp.setPodsPending(
            pods.stream().filter(p -> "Pending".equals(p.getStatus().getPhase())).count());
        resp.setPodsFailed(
            pods.stream().filter(p -> "Failed".equals(p.getStatus().getPhase())).count());
        resp.setNamespaces((long) client.namespaces().list().getItems().size());

        long cpuAlloc = nodes.stream()
            .mapToLong(n -> K8sQuantityUtils.cpuToMillis(n.getStatus().getAllocatable().get("cpu")))
            .sum();
        long memAlloc = nodes.stream()
            .mapToLong(n -> K8sQuantityUtils.memoryToBytes(n.getStatus().getAllocatable().get("memory")))
            .sum();
        resp.setCpuAllocatableMillis(cpuAlloc);
        resp.setMemAllocatableBytes(memAlloc);

        List<NodeMetrics> nodeMetrics = metricsCall(cluster,
            c -> c.top().nodes().metrics().getItems());
        if (nodeMetrics != null) {
            resp.setCpuUsedMillis(nodeMetrics.stream()
                .mapToLong(m -> K8sQuantityUtils.cpuToMillis(m.getUsage().get("cpu"))).sum());
            resp.setMemUsedBytes(nodeMetrics.stream()
                .mapToLong(m -> K8sQuantityUtils.memoryToBytes(m.getUsage().get("memory"))).sum());
            resp.setMetricsServerAvailable(true);
        } else {
            // metrics-server missing or unavailable: degrade to state-only view
            resp.setCpuUsedMillis(-1L);
            resp.setMemUsedBytes(-1L);
            resp.setMetricsServerAvailable(false);
        }
        return resp;
    }

    // ---------- nodes ----------
    @Override
    public List<NodeResponse> nodes(K8sCluster cluster) {
        KubernetesClient client = k8sClientManager.getOrCreate(cluster.getId(), cluster.getKubeconfig());
        List<Node> nodes = client.nodes().list().getItems();
        List<NodeMetrics> metricsList = metricsCall(cluster,
            c -> c.top().nodes().metrics().getItems());
        Map<String, NodeMetrics> metricsByName = metricsList == null
            ? Map.of()
            : metricsList.stream()
                .collect(Collectors.toMap(m -> m.getMetadata().getName(), m -> m, (a, b) -> a));
        return nodes.stream().map(node -> {
            NodeResponse r = new NodeResponse();
            r.setName(node.getMetadata().getName());
            r.setRoles(StringUtils.join(
                node.getMetadata().getLabels() == null ? new ArrayList<String>()
                    : node.getMetadata().getLabels().keySet().stream()
                        .filter(k -> k.startsWith(LABEL_NODE_ROLE_PREFIX))
                        .map(k -> k.substring(LABEL_NODE_ROLE_PREFIX.length()))
                        .collect(Collectors.toList()),
                ","));
            if (node.getStatus().getNodeInfo() != null) {
                r.setVersion(node.getStatus().getNodeInfo().getKubeletVersion());
            }
            r.setStatus(isNodeReady(node) ? "Ready" : "NotReady");
            if (node.getStatus().getAddresses() != null) {
                r.setInternalIp(node.getStatus().getAddresses().stream()
                    .filter(a -> "InternalIP".equals(a.getType()))
                    .map(NodeAddress::getAddress).findFirst().orElse(null));
            }
            r.setCpuAllocatableMillis(
                K8sQuantityUtils.cpuToMillis(node.getStatus().getAllocatable().get("cpu")));
            r.setMemAllocatableBytes(
                K8sQuantityUtils.memoryToBytes(node.getStatus().getAllocatable().get("memory")));
            NodeMetrics m = metricsByName.get(node.getMetadata().getName());
            if (m != null) {
                r.setCpuUsedMillis(K8sQuantityUtils.cpuToMillis(m.getUsage().get("cpu")));
                r.setMemUsedBytes(K8sQuantityUtils.memoryToBytes(m.getUsage().get("memory")));
            }
            return r;
        }).collect(Collectors.toList());
    }

    // ---------- deployments ----------
    @Override
    public List<DeploymentResponse> deployments(K8sCluster cluster, String namespace, String keyword) {
        KubernetesClient client = k8sClientManager.getOrCreate(cluster.getId(), cluster.getKubeconfig());
        List<Deployment> list = StringUtils.isBlank(namespace)
            ? client.apps().deployments().inAnyNamespace().list().getItems()
            : client.apps().deployments().inNamespace(namespace).list().getItems();
        Map<String, FlinkApplication> appByDeploy = streamParkAppIndex();
        return list.stream()
            .filter(d -> StringUtils.isBlank(keyword)
                || d.getMetadata().getName().contains(keyword))
            .map(d -> {
                DeploymentResponse r = new DeploymentResponse();
                r.setName(d.getMetadata().getName());
                r.setNamespace(d.getMetadata().getNamespace());
                r.setReplicasDesired(
                    d.getSpec().getReplicas() == null ? 0 : d.getSpec().getReplicas());
                r.setReplicasReady(
                    d.getStatus() == null || d.getStatus().getReadyReplicas() == null
                        ? 0
                        : d.getStatus().getReadyReplicas().intValue());
                r.setCreationTimestamp(d.getMetadata().getCreationTimestamp());
                if (d.getSpec().getTemplate().getSpec() != null
                    && CollectionUtils.isNotEmpty(d.getSpec().getTemplate().getSpec().getContainers())) {
                    r.setImage(d.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
                }
                FlinkApplication app = appByDeploy.get(r.getNamespace() + "/" + r.getName());
                if (app != null) {
                    r.setStreamParkAppId(app.getId());
                    r.setStreamParkAppName(app.getJobName());
                }
                return r;
            })
            .collect(Collectors.toList());
    }

    // ---------- pods ----------
    @Override
    public List<PodResponse> pods(K8sCluster cluster, String namespace, String keyword) {
        KubernetesClient client = k8sClientManager.getOrCreate(cluster.getId(), cluster.getKubeconfig());
        List<Pod> list = StringUtils.isBlank(namespace)
            ? client.pods().inAnyNamespace().list().getItems()
            : client.pods().inNamespace(namespace).list().getItems();
        // metrics lookup scoped to the same namespace when one is given
        List<PodMetrics> podMetricsList = metricsCall(cluster,
            c -> StringUtils.isBlank(namespace)
                ? c.top().pods().metrics().getItems()
                : c.top().pods().inNamespace(namespace).metrics().getItems());
        Map<String, PodMetrics> podMetrics = podMetricsList == null
            ? Map.of()
            : podMetricsList.stream()
                .collect(Collectors.toMap(
                    m -> m.getMetadata().getNamespace() + "/" + m.getMetadata().getName(),
                    m -> m, (a, b) -> a));
        return list.stream()
            .filter(p -> StringUtils.isBlank(keyword)
                || p.getMetadata().getName().contains(keyword))
            .map(p -> {
                PodResponse r = new PodResponse();
                r.setName(p.getMetadata().getName());
                r.setNamespace(p.getMetadata().getNamespace());
                r.setNodeName(p.getSpec() == null ? null : p.getSpec().getNodeName());
                r.setPhase(p.getStatus().getPhase());
                r.setIp(p.getStatus().getPodIP());
                r.setLabels(p.getMetadata().getLabels());
                r.setCreationTimestamp(p.getMetadata().getCreationTimestamp());
                if (CollectionUtils.isNotEmpty(p.getStatus().getContainerStatuses())) {
                    r.setRestarts(p.getStatus().getContainerStatuses().stream()
                        .mapToLong(s -> s.getRestartCount() == null ? 0 : s.getRestartCount().longValue())
                        .sum());
                }
                PodMetrics m = podMetrics.get(r.getNamespace() + "/" + r.getName());
                if (m != null) {
                    r.setCpuMillis(m.getContainers().stream()
                        .mapToLong(c -> K8sQuantityUtils.cpuToMillis(c.getUsage().get("cpu"))).sum());
                    r.setMemoryBytes(m.getContainers().stream()
                        .mapToLong(c -> K8sQuantityUtils.memoryToBytes(c.getUsage().get("memory"))).sum());
                }
                return r;
            })
            .collect(Collectors.toList());
    }

    // ---------- events ----------
    @Override
    public List<EventResponse> events(K8sCluster cluster, String namespace, int limit) {
        KubernetesClient client = k8sClientManager.getOrCreate(cluster.getId(), cluster.getKubeconfig());
        List<Event> list = StringUtils.isBlank(namespace)
            ? client.v1().events().inAnyNamespace().withField("type", "Warning").list().getItems()
            : client.v1().events().inNamespace(namespace).withField("type", "Warning").list().getItems();
        // lastTimestamp is ISO8601 string: lexicographic order == chronological order
        return list.stream()
            .sorted(Comparator.comparing(Event::getLastTimestamp,
                Comparator.nullsFirst(Comparator.naturalOrder())).reversed())
            .limit(limit)
            .map(e -> {
                EventResponse r = new EventResponse();
                r.setType(e.getType());
                r.setReason(e.getReason());
                if (e.getInvolvedObject() != null) {
                    r.setObject(e.getInvolvedObject().getKind() + "/" + e.getInvolvedObject().getName());
                }
                r.setMessage(e.getMessage());
                r.setCount(e.getCount());
                r.setLastTimestamp(e.getLastTimestamp());
                return r;
            })
            .collect(Collectors.toList());
    }

    // ---------- namespaces ----------
    @Override
    public List<String> namespaces(K8sCluster cluster) {
        KubernetesClient client = k8sClientManager.getOrCreate(cluster.getId(), cluster.getKubeconfig());
        return client.namespaces().list().getItems().stream()
            .map(Namespace::getMetadata)
            .map(meta -> meta == null ? null : meta.getName())
            .collect(Collectors.toList());
    }

    // ---------- history ----------
    @Override
    public HistoryResponse history(K8sCluster cluster, String kind, String name, String namespace, String range) {
        ApiAlertException.throwIfTrue(
            StringUtils.isBlank(cluster.getPrometheusUrl()),
            "Prometheus is not configured for this cluster.");
        ApiAlertException.throwIfTrue(
            !"cluster".equals(kind) && (name == null || !K8S_NAME_PATTERN.matcher(name).matches()),
            "Invalid resource name for history query.");
        ApiAlertException.throwIfTrue(
            "pod".equals(kind) && (namespace == null || !K8S_NAME_PATTERN.matcher(namespace).matches()),
            "Invalid namespace for history query.");

        long seconds = RANGE_SECONDS.getOrDefault(range, 21600L);
        long end = System.currentTimeMillis() / 1000;
        long start = end - seconds;
        int step = Math.max((int) (seconds / 60), 30);

        String cpuQuery;
        String memQuery;
        switch (kind) {
            case "node":
                cpuQuery = String.format(
                    "sum(rate(container_cpu_usage_seconds_total{node=\"%s\"}[5m])) * 1000",
                    promLabel(name));
                memQuery = String.format(
                    "sum(container_memory_working_set_bytes{node=\"%s\"})", promLabel(name));
                break;
            case "pod":
                cpuQuery = String.format(
                    "sum(rate(container_cpu_usage_seconds_total{namespace=\"%s\",pod=\"%s\"}[5m])) * 1000",
                    promLabel(namespace), promLabel(name));
                memQuery = String.format(
                    "sum(container_memory_working_set_bytes{namespace=\"%s\",pod=\"%s\"})",
                    promLabel(namespace), promLabel(name));
                break;
            default:
                cpuQuery = "sum(rate(container_cpu_usage_seconds_total"
                    + "{container!=\"\",container!=\"POD\"}[5m])) * 1000";
                memQuery = "sum(container_memory_working_set_bytes{container!=\"\",container!=\"POD\"})";
                break;
        }
        HistoryResponse resp = new HistoryResponse();
        resp.setCpuSeries(K8sPrometheusClient.queryRange(
            cluster.getPrometheusUrl(), cpuQuery, start, end, step));
        resp.setMemSeries(K8sPrometheusClient.queryRange(
            cluster.getPrometheusUrl(), memQuery, start, end, step));
        return resp;
    }

    // ---------- flink apps ----------
    @Override
    public List<FlinkAppResponse> flinkApps(K8sCluster cluster) {
        List<FlinkApplication> apps = loadK8sApps();
        Map<Long, JobStatusCV> statusByAppId = safeMap(() -> {
            Map<CacheKey, JobStatusCV> jobStatuses = flinkK8sWatcher.getAllJobStatus();
            Map<Long, JobStatusCV> byId = new HashMap<>();
            if (jobStatuses != null) {
                jobStatuses.forEach((key, value) -> byId.put(key.key(), value));
            }
            return byId;
        });
        List<PodResponse> clusterPods = safeList(() -> pods(cluster, null, null));
        Set<String> clusterDeployKeys = safeList(() -> deployments(cluster, null, null)).stream()
            .map(d -> d.getNamespace() + "/" + d.getName())
            .collect(Collectors.toSet());

        Map<Long, FlinkCluster> flinkClustersById = loadFlinkClustersById(apps);
        List<FlinkAppResponse> result = new ArrayList<>();
        for (FlinkApplication app : apps) {
            String deployKey = deploymentKeyOf(app, flinkClustersById);
            // only list apps actually running on the selected cluster
            if (deployKey == null || !clusterDeployKeys.contains(deployKey)) {
                continue;
            }
            int slash = deployKey.indexOf('/');
            FlinkAppResponse r = new FlinkAppResponse();
            r.setId(app.getId());
            r.setAppName(app.getJobName());
            r.setK8sNamespace(app.getK8sNamespace());
            r.setDeployMode(app.getDeployMode());
            JobStatusCV status = statusByAppId.get(app.getId());
            if (status != null && status.jobState() != null) {
                r.setJobState(status.jobState().name());
            }
            PodResponse pod = matchAppPod(
                clusterPods, deployKey.substring(0, slash), deployKey.substring(slash + 1));
            if (pod != null) {
                r.setCpuMillis(pod.getCpuMillis());
                r.setMemoryBytes(pod.getMemoryBytes());
                r.setRestarts(pod.getRestarts());
                r.setPodPhase(pod.getPhase());
            }
            result.add(r);
        }
        return result;
    }

    // ---------- private ----------
    private boolean isNodeReady(Node node) {
        if (node.getStatus().getConditions() == null) {
            return false;
        }
        return node.getStatus().getConditions().stream()
            .anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()));
    }

    private List<FlinkApplication> loadK8sApps() {
        return flinkApplicationMapper.selectList(new LambdaQueryWrapper<FlinkApplication>()
            .in(FlinkApplication::getDeployMode, FlinkDeployMode.getKubernetesMode()));
    }

    private Map<Long, FlinkCluster> loadFlinkClustersById(List<FlinkApplication> apps) {
        Set<Long> clusterIds = apps.stream()
            .map(FlinkApplication::getFlinkClusterId)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
        if (clusterIds.isEmpty()) {
            return new HashMap<>();
        }
        return flinkClusterService.listByIds(clusterIds).stream()
            .collect(Collectors.toMap(FlinkCluster::getId, c -> c, (a, b) -> a));
    }

    /** Index of this.streamPark-managed k8s apps keyed by {@code namespace/deploymentName}. */
    private Map<String, FlinkApplication> streamParkAppIndex() {
        List<FlinkApplication> apps = loadK8sApps();
        Map<Long, FlinkCluster> flinkClustersById = loadFlinkClustersById(apps);
        Map<String, FlinkApplication> index = new HashMap<>();
        for (FlinkApplication app : apps) {
            String key = deploymentKeyOf(app, flinkClustersById);
            if (key != null) {
                index.put(key, app);
            }
        }
        return index;
    }

    /**
     * Deployment identity of a StreamPark app on kubernetes: application mode deploys under the
     * jobName, session mode under the referenced FlinkCluster's clusterId (see
     * FlinkApplicationActionServiceImpl.getNamespaceClusterId).
     */
    @VisibleForTesting
    public static String deploymentKeyOf(FlinkApplication app, Map<Long, FlinkCluster> flinkClustersById) {
        if (FlinkDeployMode.KUBERNETES_NATIVE_APPLICATION == FlinkDeployMode.of(app.getDeployMode())) {
            return StringUtils.isBlank(app.getJobName())
                ? null
                : app.getK8sNamespace() + "/" + app.getJobName();
        }
        FlinkCluster flinkCluster =
            app.getFlinkClusterId() == null ? null : flinkClustersById.get(app.getFlinkClusterId());
        return flinkCluster == null ? null : flinkCluster.getK8sNamespace() + "/" + flinkCluster.getClusterId();
    }

    /**
     * Matches an app's pod on the selected cluster: exact {@code app=<deploymentName>} label first
     * (flink-native pods carry it), then the deployment→pod name-prefix convention as fallback.
     */
    private static PodResponse matchAppPod(List<PodResponse> pods, String namespace, String deploymentName) {
        if (StringUtils.isBlank(deploymentName)) {
            return null;
        }
        for (PodResponse pod : pods) {
            if (StringUtils.equals(pod.getNamespace(), namespace)
                && pod.getLabels() != null
                && deploymentName.equals(pod.getLabels().get("app"))) {
                return pod;
            }
        }
        for (PodResponse pod : pods) {
            if (StringUtils.equals(pod.getNamespace(), namespace)
                && pod.getName() != null
                && (pod.getName().equals(deploymentName)
                    || (pod.getName().startsWith(deploymentName)
                        && pod.getName().charAt(deploymentName.length()) == '-'))) {
                return pod;
            }
        }
        return null;
    }

    /** Escapes a label value so it cannot break out of the PromQL string matcher. */
    private static String promLabel(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "");
    }

    private static <T> List<T> safeList(Supplier<List<T>> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private static <K, V> Map<K, V> safeMap(Supplier<Map<K, V>> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            return new HashMap<>();
        }
    }
}
