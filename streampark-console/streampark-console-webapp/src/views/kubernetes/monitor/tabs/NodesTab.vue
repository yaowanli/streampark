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
    <ATable
      :columns="columns"
      :data-source="nodes"
      :loading="loading"
      row-key="name"
      size="small"
      :pagination="false"
    >
      <template #bodyCell="{ column, record }">
        <template v-if="column.dataIndex === 'status'">
          <ABadge :status="record.status === 'Ready' ? 'success' : 'error'" :text="record.status" />
        </template>
        <template v-else-if="column.dataIndex === 'cpu'">
          <template v-if="record.cpuUsedMillis != null && record.cpuUsedMillis >= 0">
            {{ formatCpu(record.cpuUsedMillis) }} / {{ formatCpu(record.cpuAllocatableMillis) }}
          </template>
          <template v-else>{{ formatCpu(record.cpuAllocatableMillis) }}</template>
        </template>
        <template v-else-if="column.dataIndex === 'memory'">
          <template v-if="record.memUsedBytes != null && record.memUsedBytes >= 0">
            {{ formatMemory(record.memUsedBytes) }} / {{ formatMemory(record.memAllocatableBytes) }}
          </template>
          <template v-else>{{ formatMemory(record.memAllocatableBytes) }}</template>
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
      kind="node"
      :name="trendName"
    />
  </div>
</template>
<script lang="ts">
  import { defineComponent, ref, watch } from 'vue';
  import { Badge, Button, Table } from 'ant-design-vue';
  import { fetchK8sNodes } from '/@/api/kubernetes/monitor';
  import type { K8sNode } from '/@/api/kubernetes/monitor.type';
  import { useI18n } from '/@/hooks/web/useI18n';
  import TrendDrawer from '../components/TrendDrawer.vue';
  import { formatCpu, formatMemory } from '../utils';

  export default defineComponent({
    name: 'K8sNodesTab',
    components: { ABadge: Badge, AButton: Button, ATable: Table, TrendDrawer },
    props: {
      cluster: { type: Object as any, default: undefined },
      reloadKey: { type: Number, default: 0 },
    },
    setup(props) {
      const { t } = useI18n();
      const nodes = ref<K8sNode[]>([]);
      const loading = ref(false);
      const trendVisible = ref(false);
      const trendName = ref('');

      const columns = [
        { title: t('kubernetes.monitor.node'), dataIndex: 'name' },
        { title: 'Roles', dataIndex: 'roles' },
        { title: 'Version', dataIndex: 'version' },
        { title: t('kubernetes.monitor.phase'), dataIndex: 'status' },
        { title: 'IP', dataIndex: 'internalIp' },
        { title: 'CPU', dataIndex: 'cpu' },
        { title: t('kubernetes.monitor.memory'), dataIndex: 'memory' },
        { title: '', dataIndex: 'action', width: 90 },
      ];

      const load = async () => {
        if (!props.cluster?.id || loading.value) return;
        loading.value = true;
        try {
          nodes.value = (await fetchK8sNodes(props.cluster.id)) || [];
        } finally {
          loading.value = false;
        }
      };

      const openTrend = (record: K8sNode) => {
        trendName.value = record.name;
        trendVisible.value = true;
      };

      watch(
        () => [props.cluster?.id, props.reloadKey],
        () => load(),
        { immediate: true },
      );

      return {
        t,
        columns,
        nodes,
        loading,
        formatCpu,
        formatMemory,
        trendVisible,
        trendName,
        openTrend,
      };
    },
  });
</script>
