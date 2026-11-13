/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.console.kubernetes.client;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class K8sPrometheusClientTest {

    private static final String MATRIX_JSON =
        "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{},"
            + "\"values\":[[1700000000,\"1500.5\"],[1700000060,\"2500.9\"]]}]}}";

    @Test
    void testParseMatrix() {
        List<long[]> series = K8sPrometheusClient.parseMatrix(MATRIX_JSON);
        assertThat(series).hasSize(2);
        assertThat(series.get(0)).containsExactly(1700000000L, 1500L);
        assertThat(series.get(1)).containsExactly(1700000060L, 2500L);
    }

    @Test
    void testBuildRangeUrl() {
        String url = K8sPrometheusClient.buildRangeUrl(
            "http://prom:9090/", "sum(rate(container_cpu_usage_seconds_total[5m]))", 1000L, 2000L, 60);
        assertThat(url).startsWith("http://prom:9090/api/v1/query_range?query=sum%28rate%28container_cpu")
            .contains("start=1000").contains("end=2000").contains("step=60");
    }
}
