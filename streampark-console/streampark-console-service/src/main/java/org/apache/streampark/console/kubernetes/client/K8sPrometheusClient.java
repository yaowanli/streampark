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

import org.apache.streampark.common.util.HttpClientUtils;

import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.core5.util.Timeout;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Minimal Prometheus HTTP API client (query_range only). */
public final class K8sPrometheusClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private K8sPrometheusClient() {
    }

    public static String buildRangeUrl(
                                       String prometheusUrl, String query, long startSec, long endSec, int stepSec) {
        String base = prometheusUrl.endsWith("/")
            ? prometheusUrl.substring(0, prometheusUrl.length() - 1)
            : prometheusUrl;
        return base + "/api/v1/query_range?query="
            + URLEncoder.encode(query, StandardCharsets.UTF_8)
            + "&start=" + startSec + "&end=" + endSec + "&step=" + stepSec;
    }

    /** Parses a matrix result into [timestampSec, value] pairs (first series only). */
    public static List<long[]> parseMatrix(String body) {
        List<long[]> points = new ArrayList<>();
        try {
            JsonNode data = MAPPER.readTree(body).path("data").path("result");
            if (data.isArray() && data.size() > 0) {
                for (JsonNode v : data.get(0).path("values")) {
                    long ts = v.get(0).asLong();
                    double val = Double.parseDouble(v.get(1).asText());
                    points.add(new long[]{ts, (long) val});
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed to parse prometheus response: " + e.getMessage(), e);
        }
        return points;
    }

    /** Executes query_range and returns parsed series. */
    public static List<long[]> queryRange(
                                          String prometheusUrl, String query, long startSec, long endSec, int stepSec) {
        String url = buildRangeUrl(prometheusUrl, query, startSec, endSec, stepSec);
        RequestConfig config = RequestConfig.custom()
            .setConnectTimeout(Timeout.ofSeconds(5))
            .setResponseTimeout(Timeout.ofSeconds(30))
            .build();
        return parseMatrix(HttpClientUtils.httpGetRequest(url, config));
    }
}
