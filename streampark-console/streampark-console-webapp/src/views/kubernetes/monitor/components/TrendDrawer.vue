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
  <ADrawer
    :open="visible"
    :title="`${t('kubernetes.monitor.trend')} - ${name}`"
    placement="right"
    width="640"
    destroyOnClose
    @close="close"
  >
    <template v-if="prometheusConfigured">
      <div class="mb-3">
        <ARadioGroup v-model:value="range" size="small" @change="load">
          <ARadioButton v-for="r in ranges" :key="r" :value="r">{{ r }}</ARadioButton>
        </ARadioGroup>
      </div>
      <ASpin :spinning="loading">
        <div ref="cpuRef" style="height: 220px; width: 100%"></div>
        <div ref="memRef" style="height: 220px; width: 100%"></div>
      </ASpin>
    </template>
    <AEmpty v-else :description="t('kubernetes.monitor.prometheusMissing')" />
  </ADrawer>
</template>
<script lang="ts">
  import { defineComponent, onBeforeUnmount, ref, watch } from 'vue';
  import { Drawer, Empty, Radio, Spin } from 'ant-design-vue';
  import * as echarts from 'echarts';
  import { fetchK8sHistory } from '/@/api/kubernetes/monitor';
  import type { HistorySeries } from '/@/api/kubernetes/monitor.type';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { formatCpu, formatMemory } from '../utils';

  export default defineComponent({
    name: 'K8sTrendDrawer',
    components: {
      ADrawer: Drawer,
      ARadioGroup: Radio.Group,
      ARadioButton: Radio.Button,
      ASpin: Spin,
      AEmpty: Empty,
    },
    props: {
      visible: { type: Boolean, default: false },
      clusterId: { type: [Number, String] as any, default: undefined },
      prometheusConfigured: { type: Boolean, default: false },
      kind: { type: String, default: 'node' },
      name: { type: String, default: '' },
      namespace: { type: String, default: undefined as any },
    },
    setup(props, { emit }) {
      const { t } = useI18n();
      const ranges = ['1h', '6h', '24h', '7d'];
      const range = ref('6h');
      const loading = ref(false);
      const cpuRef = ref<HTMLDivElement>();
      const memRef = ref<HTMLDivElement>();
      let cpuChart: echarts.ECharts | null = null;
      let memChart: echarts.ECharts | null = null;

      const close = () => emit('update:visible', false);

      const disposeCharts = () => {
        cpuChart?.dispose();
        memChart?.dispose();
        cpuChart = null;
        memChart = null;
      };

      const renderLine = (
        el: HTMLDivElement | undefined,
        data: [number, number][],
        title: string,
        formatter: (v: number) => string,
        color: string,
      ): echarts.ECharts | null => {
        if (!el) return null;
        const chart = echarts.init(el);
        chart.setOption({
          title: { text: title, left: 'center', textStyle: { fontSize: 13 } },
          grid: { left: 70, right: 16, top: 36, bottom: 28 },
          tooltip: { trigger: 'axis', valueFormatter: (v: number) => formatter(Math.round(v)) },
          xAxis: { type: 'time' },
          yAxis: { type: 'value', axisLabel: { formatter: (v: number) => formatter(v) } },
          series: [
            {
              type: 'line',
              showSymbol: false,
              data: data.map(([ts, v]) => [ts * 1000, v]),
              areaStyle: { opacity: 0.15 },
              itemStyle: { color },
            },
          ],
        });
        return chart;
      };

      let loadSeq = 0;
      const load = async () => {
        if (!props.clusterId || !props.name) return;
        const seq = ++loadSeq;
        loading.value = true;
        try {
          const series: HistorySeries = await fetchK8sHistory(
            props.clusterId,
            props.kind as any,
            props.name,
            props.namespace,
            range.value,
          );
          if (seq !== loadSeq) return;
          disposeCharts();
          cpuChart = renderLine(
            cpuRef.value,
            series.cpuSeries || [],
            t('kubernetes.monitor.cpu'),
            formatCpu,
            '#3b82f6',
          );
          memChart = renderLine(
            memRef.value,
            series.memSeries || [],
            t('kubernetes.monitor.memory'),
            formatMemory,
            '#10b981',
          );
        } finally {
          if (seq === loadSeq) {
            loading.value = false;
          }
        }
      };

      watch(
        () => props.visible,
        (v) => {
          if (v && props.prometheusConfigured) {
            // wait for the drawer DOM to mount (destroyOnClose recreates it)
            setTimeout(load, 100);
          }
        },
      );

      onBeforeUnmount(disposeCharts);

      return { t, ranges, range, loading, cpuRef, memRef, load, close };
    },
  });
</script>
