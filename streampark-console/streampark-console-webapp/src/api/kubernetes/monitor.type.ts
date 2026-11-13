export interface ClusterOverview {
  nodesTotal: number;
  nodesReady: number;
  podsRunning: number;
  podsPending: number;
  podsFailed: number;
  namespaces: number;
  cpuAllocatableMillis: number;
  cpuUsedMillis: number;
  memAllocatableBytes: number;
  memUsedBytes: number;
  metricsServerAvailable: boolean;
}

export interface K8sNode {
  name: string;
  roles?: string;
  version?: string;
  status: string;
  internalIp?: string;
  cpuAllocatableMillis: number;
  cpuUsedMillis?: number;
  memAllocatableBytes: number;
  memUsedBytes?: number;
}

export interface K8sDeployment {
  name: string;
  namespace: string;
  replicasReady: number;
  replicasDesired: number;
  image?: string;
  creationTimestamp?: string;
  streamParkAppId?: number;
  streamParkAppName?: string;
}

export interface K8sPod {
  name: string;
  namespace: string;
  nodeName?: string;
  phase: string;
  restarts?: number;
  cpuMillis?: number;
  memoryBytes?: number;
  ip?: string;
  creationTimestamp?: string;
  labels?: Record<string, string>;
}

export interface K8sEvent {
  type?: string;
  reason?: string;
  object?: string;
  message?: string;
  count?: number;
  lastTimestamp?: string;
}

export interface FlinkAppOnK8s {
  id: number | string;
  appName: string;
  k8sNamespace: string;
  jobState?: string;
  deployMode: number;
  cpuMillis?: number;
  memoryBytes?: number;
  restarts?: number;
  podPhase?: string;
}

export interface HistorySeries {
  cpuSeries: [number, number][];
  memSeries: [number, number][];
}
