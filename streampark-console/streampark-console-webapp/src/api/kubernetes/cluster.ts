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

import { Result } from '/#/axios';
import { AxiosResponse } from 'axios';
import { defHttp } from '/@/utils/http/axios';
import type { K8sCluster, K8sClusterTestResult } from './cluster.type';

enum K8S_CLUSTER_API {
  LIST = '/k8s/cluster/list',
  GET = '/k8s/cluster/get',
  CREATE = '/k8s/cluster/create',
  UPDATE = '/k8s/cluster/update',
  DELETE = '/k8s/cluster/delete',
  TEST = '/k8s/cluster/test',
}

export function fetchK8sClusterList() {
  return defHttp.post<K8sCluster[]>({ url: K8S_CLUSTER_API.LIST });
}

export function fetchK8sClusterGet(id: number | string) {
  return defHttp.post<K8sCluster>({ url: K8S_CLUSTER_API.GET, data: { id } });
}

export function fetchK8sClusterCreate(data: Recordable) {
  return defHttp.post({ url: K8S_CLUSTER_API.CREATE, data });
}

export function fetchK8sClusterUpdate(data: Recordable) {
  return defHttp.post({ url: K8S_CLUSTER_API.UPDATE, data });
}

export function fetchK8sClusterDelete(id: number | string): Promise<AxiosResponse<Result>> {
  return defHttp.post(
    { url: K8S_CLUSTER_API.DELETE, data: { id } },
    { isReturnNativeResponse: true },
  );
}

export function fetchK8sClusterTest(data: Recordable) {
  return defHttp.post<K8sClusterTestResult>({ url: K8S_CLUSTER_API.TEST, data });
}
