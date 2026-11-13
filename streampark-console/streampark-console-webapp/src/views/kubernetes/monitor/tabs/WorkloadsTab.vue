<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<template>
  <div>
    <ARow class="mb-3" :gutter="12" align="middle">
      <ACol flex="220px">
        <ASelect
          v-model:value="namespace"
          :options="namespaceOptions"
          :placeholder="t('kubernetes.monitor.namespace')"
          allow-clear
          style="width: 100%"
          @change="loadDeployments"
        />
      </ACol>
      <ACol flex="260px">
        <AInputSearch
          v-model:value="keyword"
          :placeholder="t('kubernetes.monitor.keyword')"
          @search="loadDeployments"
        />
      </ACol>
    </ARow>

    <ATable
      :columns="deploymentColumns"
      :data-source="deployments"
      :loading="loading"
      row-key="name"
      size="small"
      :pagination="{ pageSize: 10 }"
      :row-class-name="deploymentRowClass"
    >
      <template #bodyCell="{ column, record }">
        <template v-if="column.dataIndex === 'replicas'">
          <span :style="record.replicasReady < record.replicasDesired ? 'color: #cf1322' : ''">
            {{ record.replicasReady }} / {{ record.replicasDesired }}
          </span>
        </template>
        <template v-else-if="column.dataIndex === 'streamPark'">
          <ATag v-if="record.streamParkAppId" color="blue">
            {{ t('kubernetes.monitor.streamParkManaged') }}: {{ record.streamParkAppName }}
          </ATag>
        </template>
        <template v-else-if="column.dataIndex === 'creationTimestamp'">
          {{ formatTimestamp(record.creationTimestamp) }}
        </template>
      </template>
    </ATable>

    <div class="mt-6 font-bold">{{ t('kubernetes.monitor.pod') }}</div>
    <ATable
      class="mt-2"
      :columns="podColumns"
      :data-source="pods"
      :loading="loading"
      row-key="name"
      size="small"
      :pagination="{ pageSize: 10 }"
    >
      <template #bodyCell="{ column, record }">
        <template v-if="column.dataIndex === 'phase'">
          <ABadge :status="podStatusBadge(record.phase)" :text="record.phase" />
        </template>
        <template v-else-if="column.dataIndex === 'cpuMillis'">
          {{ formatCpu(record.cpuMillis) }}
        </template>
        <template v-else-if="column.dataIndex === 'memoryBytes'">
          {{ formatMemory(record.memoryBytes) }}
        </template>
        <template v-else-if="column.dataIndex === 'creationTimestamp'">
          {{ formatTimestamp(record.creationTimestamp) }}
        </template>
        <template v-else-if="column.dataIndex === 'action'">
          <AButton type="link" size="small" @click="openTrend(record)">{{
            t('kubernetes.monitor.trend')
          }}</AButton>
        </template>
      </template>
    </ATable>

    <TrendDrawer
      v-model:visible="trendVisible"
      :cluster-id="cluster?.id"
      :prometheus-configured="!!cluster?.prometheusUrl"
      kind="pod"
      :name="trendName"
      :namespace="trendNamespace"
    />
  </div>
</template>
<script lang="ts">
  import { defineComponent, ref, watch } from 'vue';
  import { Badge, Button, Col, Input, Row, Select, Table, Tag } from 'ant-design-vue';
  import { fetchK8sDeployments, fetchK8sNamespaces, fetchK8sPods } from '/@/api/kubernetes/monitor';
  import type { K8sDeployment, K8sPod } from '/@/api/kubernetes/monitor.type';
  import { useI18n } from '/@/hooks/web/useI18n';
  import TrendDrawer from '../components/TrendDrawer.vue';
  import { formatCpu, formatMemory, formatTimestamp } from '../utils';

  export default defineComponent({
    name: 'K8sWorkloadsTab',
    components: {
      ABadge: Badge,
      AButton: Button,
      ACol: Col,
      AInputSearch: Input.Search,
      ARow: Row,
      ASelect: Select,
      ATable: Table,
      ATag: Tag,
      TrendDrawer,
    },
    props: {
      cluster: { type: Object as any, default: undefined },
      reloadKey: { type: Number, default: 0 },
    },
    setup(props) {
      const { t } = useI18n();
      const deployments = ref<K8sDeployment[]>([]);
      const pods = ref<K8sPod[]>([]);
      const loading = ref(false);
      const namespace = ref<string>();
      const keyword = ref('');
      const namespaces = ref<string[]>([]);
      const trendVisible = ref(false);
      const trendName = ref('');
      const trendNamespace = ref('');

      const deploymentColumns = [
        { title: t('kubernetes.monitor.workloads'), dataIndex: 'name' },
        { title: t('kubernetes.monitor.namespace'), dataIndex: 'namespace' },
        { title: t('kubernetes.monitor.replicas'), dataIndex: 'replicas', width: 90 },
        { title: t('kubernetes.monitor.image'), dataIndex: 'image' },
        { title: 'StreamPark', dataIndex: 'streamPark', width: 200 },
        { title: t('kubernetes.monitor.createTime'), dataIndex: 'creationTimestamp', width: 170 },
      ];

      const podColumns = [
        { title: t('kubernetes.monitor.pod'), dataIndex: 'name' },
        { title: t('kubernetes.monitor.namespace'), dataIndex: 'namespace' },
        { title: t('kubernetes.monitor.phase'), dataIndex: 'phase', width: 100 },
        { title: t('kubernetes.monitor.restarts'), dataIndex: 'restarts', width: 90 },
        { title: 'CPU', dataIndex: 'cpuMillis', width: 110 },
        { title: t('kubernetes.monitor.memory'), dataIndex: 'memoryBytes', width: 120 },
        { title: 'IP', dataIndex: 'ip', width: 120 },
        { title: t('kubernetes.monitor.node'), dataIndex: 'nodeName' },
        { title: '', dataIndex: 'action', width: 90 },
      ];

      const namespaceOptions = ref<{ label: string; value: string }[]>([]);

      const loadNamespaces = async () => {
        if (!props.cluster?.id) return;
        namespaces.value = (await fetchK8sNamespaces(props.cluster.id)) || [];
        namespaceOptions.value = namespaces.value.map((n) => ({ label: n, value: n }));
      };

      const loadDeployments = async () => {
        if (!props.cluster?.id) return;
        loading.value = true;
        try {
          deployments.value =
            (await fetchK8sDeployments(
              props.cluster.id,
              namespace.value,
              keyword.value || undefined,
            )) || [];
          pods.value =
            (await fetchK8sPods(props.cluster.id, namespace.value, keyword.value || undefined)) ||
            [];
        } finally {
          loading.value = false;
        }
      };

      const deploymentRowClass = (record: K8sDeployment) =>
        record.streamParkAppId ? 'streampark-managed-row' : '';

      const podStatusBadge = (phase: string) => {
        switch (phase) {
          case 'Running':
            return 'success';
          case 'Pending':
            return 'warning';
          case 'Failed':
            return 'error';
          default:
            return 'default';
        }
      };

      const openTrend = (record: K8sPod) => {
        trendName.value = record.name;
        trendNamespace.value = record.namespace;
        trendVisible.value = true;
      };

      watch(
        () => props.cluster?.id,
        () => loadNamespaces(),
        { immediate: true },
      );

      watch(
        () => [props.cluster?.id, props.reloadKey],
        () => loadDeployments(),
        { immediate: true },
      );

      return {
        t,
        deployments,
        pods,
        loading,
        namespace,
        keyword,
        namespaceOptions,
        deploymentColumns,
        podColumns,
        deploymentRowClass,
        podStatusBadge,
        formatCpu,
        formatMemory,
        formatTimestamp,
        trendVisible,
        trendName,
        trendNamespace,
        openTrend,
        loadDeployments,
      };
    },
  });
</script>
<style>
  .streampark-managed-row td {
    background-color: rgba(59, 130, 246, 0.06);
  }
</style>
