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
  <div ref="chartRef" :style="{ height: '190px', width: '100%' }"></div>
</template>
<script lang="ts" setup>
  import * as echarts from 'echarts';
  import { onBeforeUnmount, onMounted, ref, watch } from 'vue';

  const props = defineProps<{
    title: string;
    used: number;
    total: number;
    formatter: (v: number) => string;
  }>();

  const chartRef = ref<HTMLDivElement>();
  let chart: echarts.ECharts | null = null;

  const render = () => {
    if (!chartRef.value) return;
    if (!chart) chart = echarts.init(chartRef.value);
    const used = Math.max(props.used, 0);
    const pct = props.total > 0 && props.used >= 0 ? Math.round((used / props.total) * 100) : 0;
    chart.setOption({
      title: { text: props.title, left: 'center', textStyle: { fontSize: 13 } },
      tooltip: {
        formatter: `{b}: ${props.formatter(used)} / ${props.formatter(props.total)}`,
      },
      series: [
        {
          type: 'pie',
          radius: ['55%', '75%'],
          label: { show: true, position: 'center', formatter: `${pct}%` },
          data: [
            { value: used, name: 'Used', itemStyle: { color: '#3b82f6' } },
            {
              value: Math.max(props.total - used, 0),
              name: 'Free',
              itemStyle: { color: '#e5e7eb' },
            },
          ],
        },
      ],
    });
  };

  watch(
    () => [props.used, props.total],
    () => render(),
  );
  onMounted(render);
  onBeforeUnmount(() => {
    chart?.dispose();
    chart = null;
  });
</script>
