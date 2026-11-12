/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { defHttp } from '/@/utils/http/axios';
import type {
  ClusterOverview,
  FlinkAppOnK8s,
  HistorySeries,
  K8sDeployment,
  K8sEvent,
  K8sNode,
  K8sPod,
} from './monitor.type';

enum K8S_MONITOR_API {
  OVERVIEW = '/k8s/monitor/overview',
  NODES = '/k8s/monitor/nodes',
  WORKLOADS = '/k8s/monitor/workloads',
  PODS = '/k8s/monitor/pods',
  EVENTS = '/k8s/monitor/events',
  NAMESPACES = '/k8s/monitor/namespaces',
  HISTORY = '/k8s/monitor/history',
  FLINK_APPS = '/k8s/monitor/flinkapps',
}

const get = <T>(url: string, params: Recordable) => defHttp.get<T>({ url, params });

export const fetchK8sOverview = (clusterId: number | string) =>
  get<ClusterOverview>(K8S_MONITOR_API.OVERVIEW, { clusterId });

export const fetchK8sNodes = (clusterId: number | string) =>
  get<K8sNode[]>(K8S_MONITOR_API.NODES, { clusterId });

export const fetchK8sDeployments = (
  clusterId: number | string,
  namespace?: string,
  keyword?: string,
) => get<K8sDeployment[]>(K8S_MONITOR_API.WORKLOADS, { clusterId, namespace, keyword });

export const fetchK8sPods = (clusterId: number | string, namespace?: string, keyword?: string) =>
  get<K8sPod[]>(K8S_MONITOR_API.PODS, { clusterId, namespace, keyword });

export const fetchK8sEvents = (clusterId: number | string, namespace?: string, limit = 50) =>
  get<K8sEvent[]>(K8S_MONITOR_API.EVENTS, { clusterId, namespace, limit });

export const fetchK8sNamespaces = (clusterId: number | string) =>
  get<string[]>(K8S_MONITOR_API.NAMESPACES, { clusterId });

export const fetchK8sHistory = (
  clusterId: number | string,
  kind: 'cluster' | 'node' | 'pod',
  name?: string,
  namespace?: string,
  range = '6h',
) => get<HistorySeries>(K8S_MONITOR_API.HISTORY, { clusterId, kind, name, namespace, range });

export const fetchK8sFlinkApps = (clusterId: number | string) =>
  get<FlinkAppOnK8s[]>(K8S_MONITOR_API.FLINK_APPS, { clusterId });
