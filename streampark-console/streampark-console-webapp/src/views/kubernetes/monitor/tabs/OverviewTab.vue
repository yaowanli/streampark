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
            title="Pods"
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
  </div>
</template>
<script lang="ts">
  import { defineComponent, ref, watch } from 'vue';
  import { Alert, Card, Col, Row, Statistic } from 'ant-design-vue';
  import { fetchK8sOverview } from '/@/api/kubernetes/monitor';
  import type { ClusterOverview } from '/@/api/kubernetes/monitor.type';
  import { useI18n } from '/@/hooks/web/useI18n';
  import ResourceRingChart from '../components/ResourceRingChart.vue';
  import { formatCpu, formatMemory } from '../utils';

  export default defineComponent({
    name: 'K8sOverviewTab',
    components: {
      AAlert: Alert,
      ACard: Card,
      ACol: Col,
      ARow: Row,
      AStatistic: Statistic,
      ResourceRingChart,
    },
    props: {
      clusterId: { type: [Number, String] as any, default: undefined },
      reloadKey: { type: Number, default: 0 },
    },
    setup(props) {
      const { t } = useI18n();
      const overview = ref<ClusterOverview>();

      const load = async () => {
        if (!props.clusterId) return;
        overview.value = await fetchK8sOverview(props.clusterId);
      };

      watch(
        () => [props.clusterId, props.reloadKey],
        () => load(),
        { immediate: true },
      );

      return { overview, formatCpu, formatMemory, t };
    },
  });
</script>
