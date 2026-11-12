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
  <PageWrapper content-full-height fixed-height>
    <div class="p-4">
      <ARow class="mb-3" :gutter="12" align="middle">
        <ACol flex="260px">
          <ASelect
            v-model:value="selectedClusterId"
            :options="clusterOptions"
            :placeholder="t('kubernetes.monitor.clusters')"
            style="width: 100%"
            @change="onClusterChange"
          />
        </ACol>
        <ACol flex="none">
          <ASwitch v-model:checked="autoRefresh" checked-children="10s" />
          <span class="ml-2">{{ t('kubernetes.monitor.autoRefresh') }}</span>
        </ACol>
        <ACol flex="auto">
          <AButton type="primary" ghost size="small" @click="refresh">
            <Icon icon="ant-design:reload-outlined" />
          </AButton>
        </ACol>
      </ARow>

      <ATabs v-model:activeKey="activeTab">
        <ATabPane key="overview" :tab="t('kubernetes.monitor.overview')">
          <OverviewTab
            v-if="activeTab === 'overview'"
            :cluster-id="selectedClusterId"
            :reload-key="reloadKey"
          />
        </ATabPane>
        <ATabPane key="nodes" :tab="t('kubernetes.monitor.nodes')">
          <NodesTab
            v-if="activeTab === 'nodes'"
            :cluster="currentCluster"
            :reload-key="reloadKey"
          />
        </ATabPane>
        <ATabPane key="workloads" :tab="t('kubernetes.monitor.workloads')">
          <WorkloadsTab
            v-if="activeTab === 'workloads'"
            :cluster="currentCluster"
            :reload-key="reloadKey"
          />
        </ATabPane>
        <ATabPane key="flinkapps" :tab="t('kubernetes.monitor.flinkApps')">
          <FlinkAppsTab
            v-if="activeTab === 'flinkapps'"
            :cluster="currentCluster"
            :reload-key="reloadKey"
          />
        </ATabPane>
      </ATabs>
    </div>
  </PageWrapper>
</template>
<script lang="ts">
  import { defineComponent, computed, onUnmounted, ref, watch } from 'vue';
  import { Button, Col, Row, Select, Switch, TabPane, Tabs } from 'ant-design-vue';
  import { fetchK8sClusterList } from '/@/api/kubernetes/cluster';
  import type { K8sCluster } from '/@/api/kubernetes/cluster.type';
  import { useI18n } from '/@/hooks/web/useI18n';
  import Icon from '/@/components/Icon';
  import { PageWrapper } from '/@/components/Page';
  import OverviewTab from './tabs/OverviewTab.vue';
  import NodesTab from './tabs/NodesTab.vue';
  import WorkloadsTab from './tabs/WorkloadsTab.vue';
  import FlinkAppsTab from './tabs/FlinkAppsTab.vue';

  const REFRESH_INTERVAL_MS = 10_000;

  export default defineComponent({
    name: 'K8sMonitor',
    components: {
      AButton: Button,
      ACol: Col,
      ARow: Row,
      ASelect: Select,
      ASwitch: Switch,
      ATabPane: TabPane,
      ATabs: Tabs,
      Icon,
      PageWrapper,
      OverviewTab,
      NodesTab,
      WorkloadsTab,
      FlinkAppsTab,
    },
    setup() {
      const { t } = useI18n();
      const clusters = ref<K8sCluster[]>([]);
      const selectedClusterId = ref<number | string>();
      const autoRefresh = ref(true);
      const activeTab = ref('overview');
      const reloadKey = ref(0);

      let timer: ReturnType<typeof setInterval> | null = null;

      const clusterOptions = computed(() =>
        clusters.value.map((c) => ({ label: c.clusterName, value: c.id })),
      );

      const currentCluster = computed(() =>
        clusters.value.find((c) => c.id === selectedClusterId.value),
      );

      const loadClusters = async () => {
        clusters.value = (await fetchK8sClusterList()) || [];
        if (!selectedClusterId.value && clusters.value.length > 0) {
          selectedClusterId.value = clusters.value[0].id;
        }
      };

      const refresh = () => {
        reloadKey.value += 1;
      };

      const onClusterChange = () => refresh();

      const toggleTimer = (enabled: boolean) => {
        if (timer) {
          clearInterval(timer);
          timer = null;
        }
        if (enabled) {
          timer = setInterval(refresh, REFRESH_INTERVAL_MS);
        }
      };

      watch(autoRefresh, toggleTimer);

      loadClusters();
      toggleTimer(true);

      onUnmounted(() => toggleTimer(false));

      return {
        t,
        clusters,
        clusterOptions,
        selectedClusterId,
        currentCluster,
        autoRefresh,
        activeTab,
        reloadKey,
        refresh,
        onClusterChange,
      };
    },
  });
</script>
