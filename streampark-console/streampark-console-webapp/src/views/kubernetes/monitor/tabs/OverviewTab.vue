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
    <AAlert
      v-if="overview && !overview.metricsServerAvailable"
      type="warning"
      show-icon
      class="mb-4"
    >
      <template #message>{{ t('kubernetes.monitor.metricsMissing') }}</template>
    </AAlert>
    <ARow :gutter="16">
      <ACol :span="6">
        <ACard size="small">
          <AStatistic
            :title="t('kubernetes.monitor.readyNodes')"
            :value="overview?.nodesReady ?? 0"
            :suffix="`/ ${overview?.nodesTotal ?? 0}`"
          />
        </ACard>
      </ACol>
      <ACol :span="6">
        <ACard size="small">
          <AStatistic
            :title="t('kubernetes.monitor.runningPods')"
            :value="overview?.podsRunning ?? 0"
          />
        </ACard>
      </ACol>
      <ACol :span="6">
        <ACard size="small">
          <AStatistic
            :title="t('kubernetes.monitor.namespaceCount')"
            :value="overview?.namespaces ?? 0"
          />
        </ACard>
      </ACol>
      <ACol :span="6">
        <ACard size="small">
          <AStatistic
            :title="t('kubernetes.monitor.abnormalPods')"
            :value="`${overview?.podsPending ?? 0} / ${overview?.podsFailed ?? 0}`"
            :value-style="{ fontSize: '20px' }"
          />
        </ACard>
      </ACol>
    </ARow>
    <ARow :gutter="16" class="mt-4">
      <ACol :span="12">
        <ACard size="small">
          <ResourceRingChart
            :title="t('kubernetes.monitor.cpuAllocRate')"
            :used="overview?.cpuUsedMillis ?? -1"
            :total="overview?.cpuAllocatableMillis ?? 0"
            :formatter="formatCpu"
          />
        </ACard>
      </ACol>
      <ACol :span="12">
        <ACard size="small">
          <ResourceRingChart
            :title="t('kubernetes.monitor.memAllocRate')"
            :used="overview?.memUsedBytes ?? -1"
            :total="overview?.memAllocatableBytes ?? 0"
            :formatter="formatMemory"
          />
        </ACard>
      </ACol>
    </ARow>
    <ACard
      v-if="events.length > 0"
      size="small"
      class="mt-4"
      :title="t('kubernetes.monitor.events')"
    >
      <ATable
        :columns="eventColumns"
        :data-source="events"
        row-key="object"
        size="small"
        :pagination="false"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.dataIndex === 'lastTimestamp'">
            {{ formatTimestamp(record.lastTimestamp) }}
          </template>
        </template>
      </ATable>
    </ACard>
  </div>
</template>
<script lang="ts">
  import { defineComponent, ref, watch } from 'vue';
  import { Alert, Card, Col, Row, Statistic, Table } from 'ant-design-vue';
  import { fetchK8sEvents, fetchK8sOverview } from '/@/api/kubernetes/monitor';
  import type { ClusterOverview, K8sEvent } from '/@/api/kubernetes/monitor.type';
  import { useI18n } from '/@/hooks/web/useI18n';
  import ResourceRingChart from '../components/ResourceRingChart.vue';
  import { formatCpu, formatMemory, formatTimestamp } from '../utils';

  export default defineComponent({
    name: 'K8sOverviewTab',
    components: {
      AAlert: Alert,
      ACard: Card,
      ACol: Col,
      ARow: Row,
      AStatistic: Statistic,
      ATable: Table,
      ResourceRingChart,
    },
    props: {
      clusterId: { type: [Number, String] as any, default: undefined },
      reloadKey: { type: Number, default: 0 },
    },
    setup(props) {
      const { t } = useI18n();
      const overview = ref<ClusterOverview>();
      const events = ref<K8sEvent[]>([]);
      const loading = ref(false);

      const eventColumns = [
        { title: t('kubernetes.monitor.eventType'), dataIndex: 'type', width: 80 },
        { title: t('kubernetes.monitor.reason'), dataIndex: 'reason', width: 130 },
        { title: t('kubernetes.monitor.object'), dataIndex: 'object', width: 200 },
        { title: t('kubernetes.monitor.message'), dataIndex: 'message' },
        { title: t('kubernetes.monitor.count'), dataIndex: 'count', width: 70 },
        { title: t('kubernetes.monitor.lastSeen'), dataIndex: 'lastTimestamp', width: 160 },
      ];

      const load = async () => {
        if (!props.clusterId || loading.value) return;
        loading.value = true;
        try {
          overview.value = await fetchK8sOverview(props.clusterId);
          events.value = (await fetchK8sEvents(props.clusterId, undefined, 8)) || [];
        } finally {
          loading.value = false;
        }
      };

      watch(
        () => [props.clusterId, props.reloadKey],
        () => load(),
        { immediate: true },
      );

      return { overview, events, eventColumns, formatCpu, formatMemory, formatTimestamp, t };
    },
  });
</script>
