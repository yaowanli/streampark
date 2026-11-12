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
      :data-source="apps"
      :loading="loading"
      row-key="id"
      size="small"
      :pagination="{ pageSize: 10 }"
    >
      <template #bodyCell="{ column, record }">
        <template v-if="column.dataIndex === 'deployMode'">
          <ATag :color="record.deployMode === 6 ? 'geekblue' : 'purple'">
            {{
              record.deployMode === 6
                ? t('kubernetes.monitor.application')
                : t('kubernetes.monitor.session')
            }}
          </ATag>
        </template>
        <template v-else-if="column.dataIndex === 'jobState'">
          <ABadge
            v-if="record.jobState"
            :status="stateBadge(record.jobState)"
            :text="record.jobState"
          />
          <span v-else>—</span>
        </template>
        <template v-else-if="column.dataIndex === 'podPhase'">
          <span v-if="record.podPhase">{{ record.podPhase }}</span>
          <span v-else>—</span>
        </template>
        <template v-else-if="column.dataIndex === 'restarts'">
          {{ record.restarts ?? '—' }}
        </template>
        <template v-else-if="column.dataIndex === 'cpuMillis'">
          {{ formatCpu(record.cpuMillis) }}
        </template>
        <template v-else-if="column.dataIndex === 'memoryBytes'">
          {{ formatMemory(record.memoryBytes) }}
        </template>
      </template>
    </ATable>
    <AEmpty
      v-if="!loading && (!apps || apps.length === 0)"
      :description="t('kubernetes.monitor.empty')"
    />
  </div>
</template>
<script lang="ts">
  import { defineComponent, ref, watch } from 'vue';
  import { Badge, Empty, Table, Tag } from 'ant-design-vue';
  import { fetchK8sFlinkApps } from '/@/api/kubernetes/monitor';
  import type { FlinkAppOnK8s } from '/@/api/kubernetes/monitor.type';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { formatCpu, formatMemory } from '../utils';

  export default defineComponent({
    name: 'K8sFlinkAppsTab',
    components: { ABadge: Badge, AEmpty: Empty, ATable: Table, ATag: Tag },
    props: {
      cluster: { type: Object as any, default: undefined },
      reloadKey: { type: Number, default: 0 },
    },
    setup(props) {
      const { t } = useI18n();
      const apps = ref<FlinkAppOnK8s[]>([]);
      const loading = ref(false);

      const columns = [
        { title: t('kubernetes.monitor.appName'), dataIndex: 'appName' },
        { title: t('kubernetes.monitor.namespace'), dataIndex: 'k8sNamespace' },
        { title: t('kubernetes.monitor.deployMode'), dataIndex: 'deployMode', width: 120 },
        { title: t('kubernetes.monitor.jobState'), dataIndex: 'jobState', width: 110 },
        { title: t('kubernetes.monitor.pod'), dataIndex: 'podPhase', width: 100 },
        { title: t('kubernetes.monitor.restarts'), dataIndex: 'restarts', width: 90 },
        { title: 'CPU', dataIndex: 'cpuMillis', width: 110 },
        { title: t('kubernetes.monitor.memory'), dataIndex: 'memoryBytes', width: 120 },
      ];

      const load = async () => {
        if (!props.cluster?.id) return;
        loading.value = true;
        try {
          apps.value = (await fetchK8sFlinkApps(props.cluster.id)) || [];
        } finally {
          loading.value = false;
        }
      };

      const stateBadge = (state: string) => {
        switch (state) {
          case 'RUNNING':
            return 'success';
          case 'FINISHED':
            return 'default';
          case 'CANCELED':
          case 'POS_TERMINATED':
            return 'processing';
          case 'FAILED':
          case 'TERMINATED':
          case 'LOST':
            return 'error';
          default:
            return 'warning';
        }
      };

      watch(
        () => [props.cluster?.id, props.reloadKey],
        () => load(),
        { immediate: true },
      );

      return { columns, apps, loading, stateBadge, formatCpu, formatMemory, t };
    },
  });
</script>
