export interface K8sCluster {
  id: string | number;
  clusterName: string;
  prometheusUrl?: string;
  description?: string;
  createTime?: string;
  modifyTime?: string;
}

export interface K8sClusterTestResult {
  connected: boolean;
  version?: string;
  message?: string;
}
